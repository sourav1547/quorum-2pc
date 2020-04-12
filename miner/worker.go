// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package miner

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

const (
	// resultQueueSize is the size of channel listening to sealing result.
	resultQueueSize = 10

	// txChanSize is the size of channel listening to NewTxsEvent.
	// The number is referenced from the size of tx pool.
	txChanSize = 4096

	// chainHeadChanSize is the size of channel listening to ChainHeadEvent.
	chainHeadChanSize = 10

	// chainSideChanSize is the size of channel listening to ChainSideEvent.
	chainSideChanSize = 10

	// resubmitAdjustChanSize is the size of resubmitting interval adjustment channel.
	resubmitAdjustChanSize = 10

	// miningLogAtDepth is the number of confirmations before logging successful mining.
	miningLogAtDepth = 7

	// minRecommitInterval is the minimal time interval to recreate the mining block with
	// any newly arrived transactions.
	minRecommitInterval = 1 * time.Second

	// maxRecommitInterval is the maximum time interval to recreate the mining block with
	// any newly arrived transactions.
	maxRecommitInterval = 15 * time.Second

	// intervalAdjustRatio is the impact a single interval adjustment has on sealing work
	// resubmitting interval.
	intervalAdjustRatio = 0.1

	// intervalAdjustBias is applied during the new resubmit interval calculation in favor of
	// increasing upper limit or decreasing lower limit so that the limit can be reachable.
	intervalAdjustBias = 200 * 1000.0 * 1000.0

	// staleThreshold is the maximum depth of the acceptable stale block.
	staleThreshold = 7
)

// environment is the worker's current environment and holds all of the current state information.
type environment struct {
	signer types.Signer

	tcb       *types.TxControl
	state     *state.StateDB // apply state changes here
	ancestors mapset.Set     // ancestor set (used for checking uncle parent validity)
	family    mapset.Set     // family set (used for checking uncle invalidity)
	uncles    mapset.Set     // uncle set
	tcount    int            // tx count in cycle
	gasPool   *core.GasPool  // available gas used to pack transactions

	header   *types.Header
	txs      []*types.Transaction
	receipts []*types.Receipt

	privateReceipts []*types.Receipt
	// Leave this publicState named state, add privateState which most code paths can just ignore
	privateState *state.StateDB
}

// task contains all information for consensus engine sealing and result submitting.
type task struct {
	receipts  []*types.Receipt
	state     *state.StateDB
	block     *types.Block
	createdAt time.Time

	privateReceipts []*types.Receipt
	// Leave this publicState named state, add privateState which most code paths can just ignore
	privateState *state.StateDB
}

const (
	commitInterruptNone int32 = iota
	commitInterruptNewHead
	commitInterruptResubmit
)

// newWorkReq represents a request for new sealing work submitting with relative interrupt notifier.
type newWorkReq struct {
	interrupt *int32
	noempty   bool
	timestamp int64
}

// intervalAdjust represents a resubmitting interval adjustment.
type intervalAdjust struct {
	ratio float64
	inc   bool
}

// worker is the main object which takes care of submitting new work to consensus engine
// and gathering the sealing result.
type worker struct {
	config *params.ChainConfig
	engine consensus.Engine
	eth    Backend
	chain  *core.BlockChain

	refHashLock     sync.RWMutex
	refNumberLock   sync.RWMutex
	refHash         common.Hash                      // Hash of the last knwon reference block
	refNumber       *big.Int                         // Last know reference block
	pendingCrossTxs map[common.Hash]*types.TxControl // Pending Cross shard transactions
	crossTxsMu      sync.RWMutex

	lockedAddr   map[common.Address]*types.CLock
	lockedAddrMu sync.RWMutex

	unlockedAddr map[common.Address]*types.CLock
	cLockedAddr  map[common.Address]*types.CLock

	refCrossTxs map[uint64][]common.Hash
	refCrossMu  sync.RWMutex

	promCrossTxs []common.Hash
	promCrossMu  sync.RWMutex

	foreignDataCh   chan core.ForeignDataEvent
	foreignDataSub  event.Subscription
	crossWorkCh     chan struct{}
	pendingResultCh chan struct{}
	stopProcessCh   chan struct{}
	processing      int32
	processingMu    sync.RWMutex

	gasFloor uint64
	gasCeil  uint64
	gasLimit uint64

	// Some local parameter
	nonce      uint64
	txGasLimit uint64

	// Subscriptions
	mux           *event.TypeMux
	txsCh         chan core.NewTxsEvent
	txsSub        event.Subscription
	chainHeadCh   chan core.ChainHeadEvent
	chainHeadSub  event.Subscription
	rChainHeadCh  chan core.ChainHeadEvent
	rChainHeadSub event.Subscription
	chainSideCh   chan core.ChainSideEvent
	chainSideSub  event.Subscription

	// Channels
	newWorkCh          chan *newWorkReq
	taskCh             chan *task
	resultCh           chan *types.Block
	startCh            chan struct{}
	exitCh             chan struct{}
	resubmitIntervalCh chan time.Duration
	resubmitAdjustCh   chan *intervalAdjust

	current      *environment                 // An environment for current running cycle.
	localUncles  map[common.Hash]*types.Block // A set of side blocks generated locally as the possible uncle blocks.
	remoteUncles map[common.Hash]*types.Block // A set of side blocks as the possible uncle blocks.
	unconfirmed  *unconfirmedBlocks           // A set of locally mined blocks pending canonicalness confirmations.

	mu       sync.RWMutex // The lock used to protect the coinbase and extra fields
	coinbase common.Address
	extra    []byte

	pendingMu    sync.RWMutex
	pendingTasks map[common.Hash]*task

	snapshotMu    sync.RWMutex // The lock used to protect the block snapshot and state snapshot
	snapshotBlock *types.Block
	snapshotState *state.StateDB

	// atomic status counters
	running int32 // The indicator whether the consensus engine is running or not.
	newTxs  int32 // New arrival transaction count since last sealing work submitting.

	// External functions
	isLocalBlock func(block *types.Block) bool // Function used to determine whether the specified block is mined by local miner.

	// Test hooks
	newTaskHook  func(*task)                        // Method to call upon receiving a new sealing task.
	skipSealHook func(*task) bool                   // Method to decide whether skipping the sealing.
	fullTaskHook func()                             // Method to call before pushing the full sealing task.
	resubmitHook func(time.Duration, time.Duration) // Method to call upon updating resubmitting interval.
}

func newWorker(config *params.ChainConfig, engine consensus.Engine, eth Backend, mux *event.TypeMux, recommit time.Duration, gasFloor, gasCeil uint64, isLocalBlock func(*types.Block) bool, pendingCrossTxs map[common.Hash]*types.TxControl, crossTxsMu sync.RWMutex, refCrossTxs map[uint64][]common.Hash, refCrossMu sync.RWMutex, promCrossTxs []common.Hash, promCrossMu sync.RWMutex, lockedAddr map[common.Address]*types.CLock, lockedAddrMu sync.RWMutex) *worker {
	worker := &worker{
		config:             config,
		engine:             engine,
		eth:                eth,
		mux:                mux,
		chain:              eth.BlockChain(),
		refNumber:          big.NewInt(0),
		refHash:            eth.BlockChain().GetGenesisHash(),
		gasFloor:           gasFloor,
		gasCeil:            gasCeil,
		isLocalBlock:       isLocalBlock,
		localUncles:        make(map[common.Hash]*types.Block),
		remoteUncles:       make(map[common.Hash]*types.Block),
		unconfirmed:        newUnconfirmedBlocks(eth.BlockChain(), miningLogAtDepth),
		pendingTasks:       make(map[common.Hash]*task),
		txsCh:              make(chan core.NewTxsEvent, txChanSize),
		chainHeadCh:        make(chan core.ChainHeadEvent, chainHeadChanSize),
		rChainHeadCh:       make(chan core.ChainHeadEvent, chainHeadChanSize),
		chainSideCh:        make(chan core.ChainSideEvent, chainSideChanSize),
		newWorkCh:          make(chan *newWorkReq),
		taskCh:             make(chan *task),
		resultCh:           make(chan *types.Block, resultQueueSize),
		exitCh:             make(chan struct{}),
		startCh:            make(chan struct{}, 1),
		resubmitIntervalCh: make(chan time.Duration),
		resubmitAdjustCh:   make(chan *intervalAdjust, resubmitAdjustChanSize),
		pendingCrossTxs:    pendingCrossTxs,
		crossTxsMu:         crossTxsMu,
		lockedAddr:         lockedAddr,
		lockedAddrMu:       lockedAddrMu,
		unlockedAddr:       make(map[common.Address]*types.CLock),
		cLockedAddr:        make(map[common.Address]*types.CLock),
		refCrossTxs:        refCrossTxs,
		refCrossMu:         refCrossMu,
		foreignDataCh:      make(chan core.ForeignDataEvent),
		crossWorkCh:        make(chan struct{}),
		pendingResultCh:    make(chan struct{}),
		stopProcessCh:      make(chan struct{}),
		promCrossTxs:       promCrossTxs,
		processingMu:       promCrossMu,
	}

	if _, ok := engine.(consensus.Istanbul); ok || !config.IsQuorum || config.Clique != nil {
		// Subscribe NewTxsEvent for tx pool
		worker.txsSub = eth.TxPool().SubscribeNewTxsEvent(worker.txsCh)
		// Subscribe events for blockchain
		worker.chainHeadSub = eth.BlockChain().SubscribeChainHeadEvent(worker.chainHeadCh)
		worker.chainSideSub = eth.BlockChain().SubscribeChainSideEvent(worker.chainSideCh)
		worker.rChainHeadSub = eth.RefChain().SubscribeChainHeadEvent(worker.rChainHeadCh)
		worker.foreignDataSub = eth.BlockChain().SubscribeForeignDataEvent(worker.foreignDataCh)

		// Fixing the gas limit for the entire blockchain.
		worker.gasLimit = core.CalcGasLimit(worker.chain.GetBlockByNumber(uint64(0)), worker.gasFloor, worker.gasCeil)
		worker.txGasLimit = worker.gasLimit / uint64(100)
		worker.nonce = uint64(0) // Number of cross-shard transaction approved so far

		// Sanitize recommit interval if the user-specified one is too short.
		if recommit < minRecommitInterval {
			log.Warn("Sanitizing miner recommit interval", "provided", recommit, "updated", minRecommitInterval)
			recommit = minRecommitInterval
		}

		go worker.mainLoop()
		go worker.newWorkLoop(recommit)
		go worker.resultLoop()
		go worker.taskLoop()

		// if worker.eth.MyShard() > uint64(0) {
		// 	go worker.crossTaskLoop()
		// }

		// Submit first work to initialize pending state.
		worker.startCh <- struct{}{}
	}

	return worker
}

func (w *worker) getRefNumber() *big.Int {
	w.refNumberLock.RLock()
	defer w.refNumberLock.RUnlock()
	return w.refNumber
}

func (w *worker) getRefNumberU64() uint64 {
	w.refNumberLock.RLock()
	defer w.refNumberLock.RUnlock()
	return w.refNumber.Uint64()
}

func (w *worker) getRefHash() common.Hash {
	w.refHashLock.RLock()
	defer w.refHashLock.RUnlock()
	return w.refHash
}

func (w *worker) setRefNumber(num *big.Int) {
	w.refNumberLock.Lock()
	defer w.refNumberLock.Unlock()
	w.refNumber = num
}

func (w *worker) setRefHash(hash common.Hash) {
	w.refHashLock.Lock()
	defer w.refHashLock.Unlock()
	w.refHash = hash
}

// setEtherbase sets the etherbase used to initialize the block coinbase field.
func (w *worker) setEtherbase(addr common.Address) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.coinbase = addr
}

// setExtra sets the content used to initialize the block extra field.
func (w *worker) setExtra(extra []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.extra = extra
}

// setRecommitInterval updates the interval for miner sealing work recommitting.
func (w *worker) setRecommitInterval(interval time.Duration) {
	w.resubmitIntervalCh <- interval
}

// pending returns the pending state and corresponding block.
func (w *worker) pending() (*types.Block, *state.StateDB, *state.StateDB) {
	// return a snapshot to avoid contention on currentMu mutex
	w.snapshotMu.RLock()
	defer w.snapshotMu.RUnlock()
	if w.snapshotState == nil {
		return nil, nil, nil
	}
	return w.snapshotBlock, w.snapshotState.Copy(), w.current.privateState.Copy()
}

// pendingBlock returns pending block.
func (w *worker) pendingBlock() *types.Block {
	// return a snapshot to avoid contention on currentMu mutex
	w.snapshotMu.RLock()
	defer w.snapshotMu.RUnlock()
	return w.snapshotBlock
}

// start sets the running status as 1 and triggers new work submitting.
func (w *worker) start() {
	atomic.StoreInt32(&w.running, 1)
	if istanbul, ok := w.engine.(consensus.Istanbul); ok {
		istanbul.Start(w.chain, w.chain.CurrentBlock, w.chain.HasBadBlock)
	}
	w.startCh <- struct{}{}
}

// stop sets the running status as 0.
func (w *worker) stop() {
	if istanbul, ok := w.engine.(consensus.Istanbul); ok {
		istanbul.Stop()
	}
	atomic.StoreInt32(&w.running, 0)
}

// isRunning returns an indicator whether worker is running or not.
func (w *worker) isRunning() bool {
	return atomic.LoadInt32(&w.running) == 1
}

// close terminates all background threads maintained by the worker.
// Note the worker does not support being closed multiple times.
func (w *worker) close() {
	close(w.exitCh)
}

// newWorkLoop is a standalone goroutine to submit new mining work upon received events.
func (w *worker) newWorkLoop(recommit time.Duration) {
	var (
		interrupt   *int32
		minRecommit = recommit // minimal resubmit interval specified by user.
		timestamp   int64      // timestamp for each round of mining.
	)

	timer := time.NewTimer(0)
	<-timer.C // discard the initial tick

	// commit aborts in-flight transaction execution with given signal and resubmits a new one.
	commit := func(noempty bool, s int32) {
		if interrupt != nil {
			atomic.StoreInt32(interrupt, s)
		}
		interrupt = new(int32)
		w.newWorkCh <- &newWorkReq{interrupt: interrupt, noempty: noempty, timestamp: timestamp}
		timer.Reset(recommit)
		atomic.StoreInt32(&w.newTxs, 0)
	}
	// recalcRecommit recalculates the resubmitting interval upon feedback.
	recalcRecommit := func(target float64, inc bool) {
		var (
			prev = float64(recommit.Nanoseconds())
			next float64
		)
		if inc {
			next = prev*(1-intervalAdjustRatio) + intervalAdjustRatio*(target+intervalAdjustBias)
			// Recap if interval is larger than the maximum time interval
			if next > float64(maxRecommitInterval.Nanoseconds()) {
				next = float64(maxRecommitInterval.Nanoseconds())
			}
		} else {
			next = prev*(1-intervalAdjustRatio) + intervalAdjustRatio*(target-intervalAdjustBias)
			// Recap if interval is less than the user specified minimum
			if next < float64(minRecommit.Nanoseconds()) {
				next = float64(minRecommit.Nanoseconds())
			}
		}
		recommit = time.Duration(int64(next))
	}
	// clearPending cleans the stale pending tasks.
	clearPending := func(number uint64) {
		w.pendingMu.Lock()
		for h, t := range w.pendingTasks {
			if t.block.NumberU64()+staleThreshold <= number {
				delete(w.pendingTasks, h)
			}
		}
		w.pendingMu.Unlock()
	}

	for {
		select {
		case <-w.startCh:
			clearPending(w.chain.CurrentBlock().NumberU64())
			timestamp = time.Now().Unix()
			commit(false, commitInterruptNewHead)

		case head := <-w.chainHeadCh:
			if h, ok := w.engine.(consensus.Handler); ok {
				h.NewChainHead()
			}
			clearPending(head.Block.NumberU64())
			timestamp = time.Now().Unix()
			commit(false, commitInterruptNewHead)

		case head := <-w.rChainHeadCh:
			block := head.Block
			// w.mux.Post(core.NewRefBlockEvent{Start: w.getRefNumberU64(), End: block.NumberU64()})
			w.setRefNumber(block.Number())
			w.setRefHash(block.Hash())
			commit(false, commitInterruptNewHead)

		case <-timer.C:
			// If mining is running resubmit a new work cycle periodically to pull in
			// higher priced transactions. Disable this overhead for pending blocks.
			if w.isRunning() && (w.config.Clique == nil || w.config.Clique.Period > 0) {
				// Short circuit if no new transaction arrives.
				if atomic.LoadInt32(&w.newTxs) == 0 {
					timer.Reset(recommit)
					continue
				}
				commit(true, commitInterruptResubmit)
			}

		case interval := <-w.resubmitIntervalCh:
			// Adjust resubmit interval explicitly by user.
			if interval < minRecommitInterval {
				log.Warn("Sanitizing miner recommit interval", "provided", interval, "updated", minRecommitInterval)
				interval = minRecommitInterval
			}
			log.Info("Miner recommit interval update", "from", minRecommit, "to", interval)
			minRecommit, recommit = interval, interval

			if w.resubmitHook != nil {
				w.resubmitHook(minRecommit, recommit)
			}

		case adjust := <-w.resubmitAdjustCh:
			// Adjust resubmit interval by feedback.
			if adjust.inc {
				before := recommit
				recalcRecommit(float64(recommit.Nanoseconds())/adjust.ratio, true)
				log.Trace("Increase miner recommit interval", "from", before, "to", recommit)
			} else {
				before := recommit
				recalcRecommit(float64(minRecommit.Nanoseconds()), false)
				log.Trace("Decrease miner recommit interval", "from", before, "to", recommit)
			}

			if w.resubmitHook != nil {
				w.resubmitHook(minRecommit, recommit)
			}

		case <-w.exitCh:
			return
		}
	}
}

// mainLoop is a standalone goroutine to regenerate the sealing task based on the received event.
func (w *worker) mainLoop() {
	defer w.txsSub.Unsubscribe()
	defer w.chainHeadSub.Unsubscribe()
	defer w.chainSideSub.Unsubscribe()
	defer w.rChainHeadSub.Unsubscribe()
	defer w.foreignDataSub.Unsubscribe()

	for {
		select {
		case req := <-w.newWorkCh:
			w.commitNewWork(req.interrupt, req.noempty, req.timestamp)

		case ev := <-w.chainSideCh:
			// Short circuit for duplicate side blocks
			if _, exist := w.localUncles[ev.Block.Hash()]; exist {
				continue
			}
			if _, exist := w.remoteUncles[ev.Block.Hash()]; exist {
				continue
			}
			// Add side block to possible uncle block set depending on the author.
			if w.isLocalBlock != nil && w.isLocalBlock(ev.Block) {
				w.localUncles[ev.Block.Hash()] = ev.Block
			} else {
				w.remoteUncles[ev.Block.Hash()] = ev.Block
			}
			// If our mining block contains less than 2 uncle blocks,
			// add the new uncle block if valid and regenerate a mining block.
			if w.isRunning() && w.current != nil && w.current.uncles.Cardinality() < 2 {
				start := time.Now()
				if err := w.commitUncle(w.current, ev.Block.Header()); err == nil {
					var uncles []*types.Header
					w.current.uncles.Each(func(item interface{}) bool {
						hash, ok := item.(common.Hash)
						if !ok {
							return false
						}
						uncle, exist := w.localUncles[hash]
						if !exist {
							uncle, exist = w.remoteUncles[hash]
						}
						if !exist {
							return false
						}
						uncles = append(uncles, uncle.Header())
						return false
					})
					w.commit(uncles, nil, true, start)
				}
			}

		case ev := <-w.txsCh:
			// Apply transactions to the pending state if we're not mining.
			//
			// Note all transactions received may not be continuous with transactions
			// already included in the current mining block. These transactions will
			// be automatically eliminated.
			if !w.isRunning() && w.current != nil {
				w.mu.RLock()
				coinbase := w.coinbase
				w.mu.RUnlock()

				txs := make(map[common.Address]types.Transactions)
				for _, tx := range ev.Txs {
					acc, _ := types.Sender(w.current.signer, tx)
					txs[acc] = append(txs[acc], tx)
				}
				txset := types.NewTransactionsByPriceAndNonce(w.current.signer, txs)
				w.commitTransactions(txset, coinbase, nil)
				w.updateSnapshot()
			} else {
				// If we're mining, but nothing is being processed, wake on new transactions
				if w.config.Clique != nil && w.config.Clique.Period == 0 {
					w.commitNewWork(nil, false, time.Now().Unix())
				}
			}
			atomic.AddInt32(&w.newTxs, int32(len(ev.Txs)))

		// System stopped
		case <-w.exitCh:
			return
		case <-w.txsSub.Err():
			return
		case <-w.chainHeadSub.Err():
			return
		case <-w.rChainHeadSub.Err():
			return
		case <-w.chainSideSub.Err():
			return
		}
	}
}

// taskLoop is a standalone goroutine to fetch sealing task from the generator and
// push them to consensus engine.
func (w *worker) taskLoop() {
	var (
		stopCh chan struct{}
		prev   common.Hash
	)

	// interrupt aborts the in-flight sealing task.
	interrupt := func() {
		if stopCh != nil {
			close(stopCh)
			stopCh = nil
		}
	}
	for {
		select {
		case task := <-w.taskCh:
			if w.newTaskHook != nil {
				w.newTaskHook(task)
			}
			// Reject duplicate sealing work due to resubmitting.
			sealHash := w.engine.SealHash(task.block.Header())
			if sealHash == prev {
				continue
			}
			// Interrupt previous sealing operation
			interrupt()
			stopCh, prev = make(chan struct{}), sealHash

			if w.skipSealHook != nil && w.skipSealHook(task) {
				continue
			}
			w.pendingMu.Lock()
			w.pendingTasks[w.engine.SealHash(task.block.Header())] = task
			w.pendingMu.Unlock()

			if err := w.engine.Seal(w.chain, task.block, w.resultCh, stopCh); err != nil {
				log.Warn("Block sealing failed", "err", err)
			}
		case <-w.exitCh:
			interrupt()
			return
		}
	}
}

// resultLoop is a standalone goroutine to handle sealing result submitting
// and flush relative data to the database.
func (w *worker) resultLoop() {
	for {
		select {
		case block := <-w.resultCh:
			// Short circuit when receiving empty result.
			if block == nil {
				continue
			}
			// Short circuit when receiving duplicate result caused by resubmitting.
			if w.chain.HasBlock(block.Hash(), block.NumberU64()) {
				continue
			}
			var (
				sealhash = w.engine.SealHash(block.Header())
				hash     = block.Hash()
			)
			w.pendingMu.RLock()
			task, exist := w.pendingTasks[sealhash]
			w.pendingMu.RUnlock()
			if !exist {
				log.Error("Block found but no relative pending task", "number", block.Number(), "sealhash", sealhash, "hash", hash)
				continue
			}
			// Different block could share same sealhash, deep copy here to prevent write-write conflict.
			var logs []*types.Log

			for _, receipt := range append(task.receipts, task.privateReceipts...) {
				// Update the block hash in all logs since it is now available and not when the
				// receipt/log of individual transactions were created.
				for _, log := range receipt.Logs {
					log.BlockHash = hash
				}
				logs = append(logs, receipt.Logs...)
			}

			// write private transactions
			privateStateRoot, err := task.privateState.Commit(w.config.IsEIP158(block.Number()))
			if err != nil {
				log.Error("Failed committing private state root", "err", err)
				continue
			}
			if err := core.WritePrivateStateRoot(w.eth.ChainDb(), block.Root(), privateStateRoot); err != nil {
				log.Error("Failed writing private state root", "err", err)
				continue
			}
			allReceipts := mergeReceipts(task.receipts, task.privateReceipts)

			// Commit block and state to database.
			stat, err := w.chain.WriteBlockWithState(block, allReceipts, task.state, nil)
			if err != nil {
				log.Error("Failed writing block to chain", "err", err)
				continue
			}
			if err := core.WritePrivateBlockBloom(w.eth.ChainDb(), block.NumberU64(), task.privateReceipts); err != nil {
				log.Error("Failed writing private block bloom", "err", err)
				continue
			}

			var (
				txType uint64
				tHash  common.Hash
				tcb    *types.TxControl
				ccount = uint64(0)
			)
			for _, tx := range block.Transactions() {
				txType = tx.TxType()
				tHash = tx.Hash()
				if txType == types.CrossShardLocal {
					w.crossTxsMu.RLock()
					tcb = w.pendingCrossTxs[tHash]
					w.crossTxsMu.RUnlock()

					tcb.TxControlMu.RLock()
					keyval := tcb.Keyval
					tcb.TxControlMu.RUnlock()

					for addr, keys := range keyval {
						w.lockedAddrMu.RLock()
						aclok := w.lockedAddr[addr]
						w.lockedAddrMu.RUnlock()

						aclok.ClockMu.Lock()
						for _, key := range keys.Keys {
							delete(aclok.Keys, key)
						}
						clockSize := len(aclok.Keys)
						aclok.ClockMu.Unlock()
						if clockSize == 0 {
							w.lockedAddrMu.Lock()
							delete(w.lockedAddr, addr)
							w.lockedAddrMu.Unlock()
						}
					}
				} else if txType == types.LocalDecision {
					index := 4
					data := tx.Data()
					tHash := common.BytesToHash(data[index : index+32])
					status := binary.BigEndian.Uint64(data[index+24:])
					// status: 1==commit, 0==abort
					if status == uint64(1) {
						w.crossTxsMu.RLock()
						keyVal := w.pendingCrossTxs[tHash].Keyval
						w.crossTxsMu.RUnlock()

						for addr, ckeys := range keyVal {
							w.lockedAddrMu.Lock()
							if _, ok := w.lockedAddr[addr]; !ok {
								w.lockedAddr[addr] = types.NewCLock(addr)
							}
							alock := w.lockedAddr[addr]
							w.lockedAddrMu.Unlock()

							alock.ClockMu.Lock()
							for _, key := range ckeys.Keys {
								alock.Keys[key] = false
							}
							alock.ClockMu.Unlock()
						}
					}
					ccount++
				}
			}
			w.chain.AddCount(ccount)

			log.Info("Successfully sealed new block", "number", block.Number(), "sealhash", sealhash, "hash", hash, "root", block.Root(),
				"elapsed", common.PrettyDuration(time.Since(task.createdAt)))

			// Broadcast the block and announce chain insertion event
			w.mux.Post(core.NewMinedBlockEvent{Block: block})

			var events []interface{}
			switch stat {
			case core.CanonStatTy:
				events = append(events, core.ChainEvent{Block: block, Hash: block.Hash(), Logs: logs})
				events = append(events, core.ChainHeadEvent{Block: block})
			case core.SideStatTy:
				events = append(events, core.ChainSideEvent{Block: block})
			}
			w.chain.PostChainEvents(events, logs)

			// Insert the block into the set of pending ones to resultLoop for confirmations
			w.unconfirmed.Insert(block.NumberU64(), block.Hash())

		case <-w.exitCh:
			return
		}
	}
}

// Given a slice of public receipts and an overlapping (smaller) slice of
// private receipts, return a new slice where the default for each location is
// the public receipt but we take the private receipt in each place we have
// one.
func mergeReceipts(pub, priv types.Receipts) types.Receipts {
	m := make(map[common.Hash]*types.Receipt)
	for _, receipt := range pub {
		m[receipt.TxHash] = receipt
	}
	for _, receipt := range priv {
		m[receipt.TxHash] = receipt
	}

	ret := make(types.Receipts, 0, len(pub))
	for _, pubReceipt := range pub {
		ret = append(ret, m[pubReceipt.TxHash])
	}

	return ret
}

// makeCurrent creates a new environment for the current cycle.
func (w *worker) makeCurrent(parent *types.Block, header *types.Header) error {
	var env *environment
	publicState, privateState, err := w.chain.StateAt(parent.Root())
	if err != nil {
		return err
	}
	env = &environment{
		signer:       types.MakeSigner(w.config, header.Number),
		state:        publicState,
		ancestors:    mapset.NewSet(),
		family:       mapset.NewSet(),
		uncles:       mapset.NewSet(),
		header:       header,
		privateState: privateState,
	}

	// when 08 is processed ancestors contain 07 (quick block)
	for _, ancestor := range w.chain.GetBlocksFromHash(parent.Hash(), 7) {
		for _, uncle := range ancestor.Uncles() {
			env.family.Add(uncle.Hash())
		}
		env.family.Add(ancestor.Hash())
		env.ancestors.Add(ancestor.Hash())
	}

	// Keep track of transactions which return errors so they can be removed
	w.current = env
	return nil
}

func (w *worker) getNewTransactions(start, end uint64) ([]common.Hash, []common.Hash) {
	var (
		abortTxs  []common.Hash
		commitTxs []common.Hash
		commit    bool
		cTxs      []common.Hash
	)
	for curr := start; curr <= end; curr++ {
		w.refCrossMu.RLock()
		cTxs = w.refCrossTxs[curr]
		w.refCrossMu.RUnlock()
		for _, th := range cTxs {
			commit = w.checkCommitStatus(th)
			log.Info("@pc, commit status from", "th", th, "commit", commit)
			if commit {
				commitTxs = append(commitTxs, th)
			} else {
				abortTxs = append(abortTxs, th)
			}
		}
	}
	log.Info("@pc, returing new transaction hashes", "cLen", len(commitTxs), "aLen", len(abortTxs))
	return commitTxs, abortTxs
}

func (w *worker) checkCommitStatus(th common.Hash) bool {
	w.crossTxsMu.RLock()
	cKeys := w.pendingCrossTxs[th].Keyval // map addr::keys
	w.crossTxsMu.RUnlock()
	var locked bool

	// Iterate over all address to check that all keys are available.
	for addr, ckeys := range cKeys {
		locked = w.checkLockedStatus(addr, ckeys.Keys)
		if locked {
			return false
		}
	}

	for addr, ckeys := range cKeys {
		if _, aok := w.cLockedAddr[addr]; !aok {
			w.cLockedAddr[addr] = types.NewCLock(addr)
		}
		if _, uok := w.unlockedAddr[addr]; uok {
			unlockedKeys := w.unlockedAddr[addr].Keys
			for _, key := range ckeys.Keys {
				if _, uok := unlockedKeys[key]; uok {
					delete(unlockedKeys, key)
				} else {
					w.cLockedAddr[addr].Keys[key] = false
				}
			}
		} else {
			for _, key := range ckeys.Keys {
				w.cLockedAddr[addr].Keys[key] = false
			}
		}
	}
	return true
}

func (w *worker) checkLockedStatus(addr common.Address, addrKeys []common.Hash) bool {
	w.lockedAddrMu.RLock()
	_, galok := w.lockedAddr[addr] // globally locked
	w.lockedAddrMu.RUnlock()
	_, calok := w.cLockedAddr[addr] // locally locked

	// contract not locked anywhere
	if !galok && !calok {
		return false
	}

	// contract locked globally but not locally
	if galok && !calok {
		if _, auok := w.unlockedAddr[addr]; auok {
			w.lockedAddrMu.RLock()
			lockedCLock := w.lockedAddr[addr]
			w.lockedAddrMu.RUnlock()
			unlockedKeys := w.unlockedAddr[addr].Keys

			lockedCLock.ClockMu.RLock()
			defer lockedCLock.ClockMu.RUnlock()

			lockedKeys := lockedCLock.Keys
			for _, key := range addrKeys {
				_, lkok := lockedKeys[key]
				_, ukok := unlockedKeys[key]
				// globally locked but not unlocked locally
				if lkok && !ukok {
					return true
				}
			}
			return false
		} else {
			w.lockedAddrMu.RLock()
			lockedCLock := w.lockedAddr[addr]
			w.lockedAddrMu.RUnlock()

			lockedCLock.ClockMu.RLock()
			defer lockedCLock.ClockMu.RUnlock()
			lockedKeys := lockedCLock.Keys

			for _, key := range addrKeys {
				// globally locked
				if _, lkok := lockedKeys[key]; lkok {
					return true
				}
			}
			return false
		}
	}

	// locked locally but not globally
	clKeys := w.cLockedAddr[addr].Keys
	for _, key := range addrKeys {
		// key locked locally
		if _, clkok := clKeys[key]; clkok {
			return true
		}
	}
	return false
}

func (w *worker) getPromotedTransactions() []common.Hash {
	w.promCrossMu.RLock()
	defer w.promCrossMu.RUnlock()
	var pTxs []common.Hash
	if len(w.promCrossTxs) > 0 {
		for _, th := range w.promCrossTxs {
			if w.pendingCrossTxs[th].Status {
				pTxs = append(pTxs, th)
			}
		}
	}
	log.Info("@pc, returning promoted transactions", "len", len(pTxs))
	return pTxs
}

// commitUncle adds the given block to uncle block set, returns error if failed to add.
func (w *worker) commitUncle(env *environment, uncle *types.Header) error {
	hash := uncle.Hash()
	if env.uncles.Contains(hash) {
		return errors.New("uncle not unique")
	}
	if env.header.ParentHash == uncle.ParentHash {
		return errors.New("uncle is sibling")
	}
	if !env.ancestors.Contains(uncle.ParentHash) {
		return errors.New("uncle's parent unknown")
	}
	if env.family.Contains(hash) {
		return errors.New("uncle already included")
	}
	env.uncles.Add(uncle.Hash())
	return nil
}

// updateSnapshot updates pending snapshot block and state.
// Note this function assumes the current variable is thread safe.
func (w *worker) updateSnapshot() {
	w.snapshotMu.Lock()
	defer w.snapshotMu.Unlock()

	var uncles []*types.Header
	w.current.uncles.Each(func(item interface{}) bool {
		hash, ok := item.(common.Hash)
		if !ok {
			return false
		}
		uncle, exist := w.localUncles[hash]
		if !exist {
			uncle, exist = w.remoteUncles[hash]
		}
		if !exist {
			return false
		}
		uncles = append(uncles, uncle.Header())
		return false
	})

	w.snapshotBlock = types.NewBlock(
		w.current.header,
		w.current.txs,
		uncles,
		w.current.receipts,
	)

	w.snapshotState = w.current.state.Copy()
}

func (w *worker) commitTransaction(tx *types.Transaction, coinbase common.Address) ([]*types.Log, error) {
	snap := w.current.state.Snapshot()
	privateSnap := w.current.privateState.Snapshot()

	receipt, privateReceipt, _, err := core.ApplyTransaction(w.config, w.chain, &coinbase, w.current.gasPool, nil, w.current.state, w.current.privateState, w.current.header, tx, &w.current.header.GasUsed, vm.Config{})
	if err != nil {
		w.current.state.RevertToSnapshot(snap)
		w.current.privateState.RevertToSnapshot(privateSnap)
		return nil, err
	}
	w.current.txs = append(w.current.txs, tx)
	w.current.receipts = append(w.current.receipts, receipt)

	logs := receipt.Logs
	if privateReceipt != nil {
		logs = append(receipt.Logs, privateReceipt.Logs...)
		w.current.privateReceipts = append(w.current.privateReceipts, privateReceipt)
	}
	return logs, nil
}

func (w *worker) commitInitialContract(coinbase common.Address, interrupt *int32) bool {
	if w.current == nil {
		return true
	}

	path := ""
	if w.eth.MyShard() == uint64(0) {
		path = "init-contracts0.json"
	} else {
		path = "init-contracts1.json"
	}
	file, err := os.Open(path)
	if err != nil {
		log.Error("Failed to read init-contracts file: %v", err)
		return true
	}
	defer file.Close()

	contracts := new(core.InitContracts)
	if err := json.NewDecoder(file).Decode(contracts); err != nil {
		log.Error("invalid init-contracts file", "error", err)
		return true
	}

	var coalescedLogs []*types.Log

	if w.current.gasPool == nil {
		w.current.gasPool = new(core.GasPool).AddGas(w.gasLimit)
	}

	gasPrice := big.NewInt(0)
	blkGasLimit := w.current.header.GasLimit
	gasLimit := blkGasLimit / 100
	// To check contract objects
	for _, contract := range contracts.Contracts {

		if interrupt != nil && atomic.LoadInt32(interrupt) != commitInterruptNone {
			// Notify resubmit loop to increase resubmitting interval due to too frequent commits.
			if atomic.LoadInt32(interrupt) == commitInterruptResubmit {
				ratio := float64(w.current.header.GasLimit-w.current.gasPool.Gas()) / float64(w.current.header.GasLimit)
				if ratio < 0.1 {
					ratio = 0.1
				}
				w.resubmitAdjustCh <- &intervalAdjust{
					ratio: ratio,
					inc:   true,
				}
			}
			return atomic.LoadInt32(interrupt) == commitInterruptNewHead
		}

		tx := types.NewContractCreation(types.ContractInit, contract.Nonce, w.eth.MyShard(), contract.Balance, gasLimit, gasPrice, contract.Code)

		w.current.state.Prepare(tx.Hash(), common.Hash{}, w.current.tcount)
		w.current.privateState.Prepare(tx.Hash(), common.Hash{}, w.current.tcount)

		snap := w.current.state.Snapshot()
		privateSnap := w.current.privateState.Snapshot()

		receipt, privateReceipt, _, err := core.ApplyTransaction(w.config, w.chain, &coinbase, w.current.gasPool, nil, w.current.state, w.current.privateState, w.current.header, tx, &w.current.header.GasUsed, vm.Config{})
		if err != nil {
			w.current.state.RevertToSnapshot(snap)
			w.current.privateState.RevertToSnapshot(privateSnap)
			log.Error("Contract intialiazation failed with", "error", err)
			continue
		}
		w.current.txs = append(w.current.txs, tx)
		w.current.receipts = append(w.current.receipts, receipt)

		logs := receipt.Logs
		if privateReceipt != nil {
			logs = append(receipt.Logs, privateReceipt.Logs...)
			w.current.privateReceipts = append(w.current.privateReceipts, privateReceipt)
		}

		coalescedLogs = append(coalescedLogs, logs...)
		w.current.tcount++
	}

	if w.eth.MyShard() == uint64(0) && w.current.tcount > 0 {
		w.chain.SetCommitAddress(w.current.receipts[0].ContractAddress)
	}
	if !w.isRunning() && len(coalescedLogs) > 0 {
		// We don't push the pendingLogsEvent while we are mining. The reason is that
		// when we are mining, the worker will regenerate a mining block every 3 seconds.
		// In order to avoid pushing the repeated pendingLog, we disable the pending log pushing.

		// make a copy, the state caches the logs and these logs get "upgraded" from pending to mined
		// logs by filling in the block hash when the block was mined by the local miner. This can
		// cause a race condition if a log was "upgraded" before the PendingLogsEvent is processed.
		cpy := make([]*types.Log, len(coalescedLogs))
		for i, l := range coalescedLogs {
			cpy[i] = new(types.Log)
			*cpy[i] = *l
		}
		go w.mux.Post(core.PendingLogsEvent{Logs: cpy})
	}
	// Notify resubmit loop to decrease resubmitting interval if current interval is larger
	// than the user-specified one.
	if interrupt != nil {
		w.resubmitAdjustCh <- &intervalAdjust{inc: false}
	}

	// Todo: Double check whatever it does.
	return false
}

func (w *worker) commitCrossTransactions(tHashes []common.Hash, coinbase common.Address, interrupt *int32) bool {
	if w.current == nil {
		return true
	}

	if w.current.gasPool == nil {
		w.current.gasPool = new(core.GasPool).AddGas(w.gasLimit)
	}

	var (
		tHash common.Hash
		tcb   *types.TxControl
		tx    *types.Transaction
	)
	for _, tHash = range tHashes {
		if interrupt != nil && atomic.LoadInt32(interrupt) != commitInterruptNone {
			// Notify resubmit loop to increase resubmitting interval due to too frequent commits.
			if atomic.LoadInt32(interrupt) == commitInterruptResubmit {
				ratio := float64(w.current.header.GasLimit-w.current.gasPool.Gas()) / float64(w.current.header.GasLimit)
				if ratio < 0.1 {
					ratio = 0.1
				}
				w.resubmitAdjustCh <- &intervalAdjust{
					ratio: ratio,
					inc:   true,
				}
			}
			return atomic.LoadInt32(interrupt) == commitInterruptNewHead
		}

		w.crossTxsMu.RLock()
		tcb = w.pendingCrossTxs[tHash]
		tx = tcb.Tx
		w.crossTxsMu.RUnlock()

		if !tcb.Status {
			log.Warn("Promoted trasnaction without data", "hash", tHash)
			continue
		}

		if w.current.gasPool.Gas() < params.TxGas {
			log.Trace("Not enough gas for further cross-shard transactions", "have", w.current.gasPool, "want", params.TxGas)
			break
		}

		w.current.state.Prepare(tx.Hash(), common.Hash{}, w.current.tcount)
		w.current.privateState.Prepare(tx.Hash(), common.Hash{}, w.current.tcount)

		snap := w.current.state.Snapshot()
		psnap := w.current.privateState.Snapshot()

		receipt, _, _, err := core.ApplyTransaction(w.config, w.chain, &coinbase, w.current.gasPool, tcb, w.current.state, w.current.privateState, w.current.header, tx, &w.current.header.GasUsed, vm.Config{})

		if err != nil {
			w.current.state.RevertToSnapshot(snap)
			w.current.privateState.RevertToSnapshot(psnap)
		}
		// Add the transaction even if it throws error to simply indicate
		// the fact that we have considered the transaction for execution.
		w.current.txs = append(w.current.txs, tx)
		w.current.receipts = append(w.current.receipts, receipt)
		w.current.tcount++

		// Updated the status of unlocked keys
		for addr, ckeys := range tcb.Keyval {
			if _, ok := w.unlockedAddr[addr]; !ok {
				w.unlockedAddr[addr] = types.NewCLock(addr)
			}
			for _, key := range ckeys.Keys {
				w.unlockedAddr[addr].Keys[key] = false // key:false is the default setup
			}
		}
	}
	return false
}

func (w *worker) commitNewTransactions(commit bool, tHashes []common.Hash, coinbase common.Address, interrupt *int32) bool {
	if w.current == nil {
		return true
	}

	if w.current.gasPool == nil {
		w.current.gasPool = new(core.GasPool).AddGas(w.gasLimit)
	}
	ccount := w.chain.CrossCount()
	for _, tHash := range tHashes {
		if interrupt != nil && atomic.LoadInt32(interrupt) != commitInterruptNone {
			// Notify resubmit loop to increase resubmitting interval due to too frequent commits.
			if atomic.LoadInt32(interrupt) == commitInterruptResubmit {
				ratio := float64(w.current.header.GasLimit-w.current.gasPool.Gas()) / float64(w.current.header.GasLimit)
				if ratio < 0.1 {
					ratio = 0.1
				}
				w.resubmitAdjustCh <- &intervalAdjust{
					ratio: ratio,
					inc:   true,
				}
			}
			return atomic.LoadInt32(interrupt) == commitInterruptNewHead
		}

		// commit == true, abort == false
		var (
			dataLen = 2*32 + 4
			start   = 0
		)
		data := make([]byte, dataLen)
		funcAddress, _ := hex.DecodeString("21a37d8a") // 21a37d8a: addDecision(bytes32,bool)
		statusByte := make([]byte, 32)
		statusByte[31] = 0
		if commit {
			statusByte[31] = 1
		}
		start += copy(data[start:], funcAddress)
		start += copy(data[start:], tHash.Bytes())
		start += copy(data[start:], statusByte)

		statusTx := types.NewTransaction(types.LocalDecision, ccount, w.eth.MyShard(), w.chain.CommitAddress(), big.NewInt(0), w.txGasLimit, big.NewInt(0), data)

		if w.current.gasPool.Gas() < params.TxGas {
			log.Warn("Not enough gas for further cross-shard transactions", "have", w.current.gasPool, "want", params.TxGas)
			break
		}
		log.Info("Posting local decision", "th", tHash, "sth", statusTx.Hash(), "val", statusTx.Value().Uint64(), "to", w.chain.CommitAddress(), "data", hex.EncodeToString(data))

		w.current.state.Prepare(statusTx.Hash(), common.Hash{}, w.current.tcount)
		w.current.privateState.Prepare(statusTx.Hash(), common.Hash{}, w.current.tcount)

		snap := w.current.state.Snapshot()
		psnap := w.current.privateState.Snapshot()

		receipt, _, _, err := core.ApplyTransaction(w.config, w.chain, &coinbase, w.current.gasPool, nil, w.current.state, w.current.privateState, w.current.header, statusTx, &w.current.header.GasUsed, vm.Config{})

		if err != nil {
			log.Warn("Error in execution status transaction", "err", err, "thash", tHash)
			w.current.state.RevertToSnapshot(snap)
			w.current.privateState.RevertToSnapshot(psnap)
		}
		// Add the transaction even if it throws error to simply indicate
		// the fact that we have considered the transaction for execution.
		w.current.txs = append(w.current.txs, statusTx)
		w.current.receipts = append(w.current.receipts, receipt)
		w.current.tcount++
		ccount++
	}
	return false
}

func (w *worker) commitTransactions(txs *types.TransactionsByPriceAndNonce, coinbase common.Address, interrupt *int32) bool {
	// Short circuit if current is nil
	if w.current == nil {
		return true
	}

	if w.current.gasPool == nil {
		w.current.gasPool = new(core.GasPool).AddGas(w.gasLimit)
	}

	var coalescedLogs []*types.Log

	for {
		// In the following three cases, we will interrupt the execution of the transaction.
		// (1) new head block event arrival, the interrupt signal is 1
		// (2) worker start or restart, the interrupt signal is 1
		// (3) worker recreate the mining block with any newly arrived transactions, the interrupt signal is 2.
		// For the first two cases, the semi-finished work will be discarded.
		// For the third case, the semi-finished work will be submitted to the consensus engine.
		if interrupt != nil && atomic.LoadInt32(interrupt) != commitInterruptNone {
			// Notify resubmit loop to increase resubmitting interval due to too frequent commits.
			if atomic.LoadInt32(interrupt) == commitInterruptResubmit {
				ratio := float64(w.current.header.GasLimit-w.current.gasPool.Gas()) / float64(w.current.header.GasLimit)
				if ratio < 0.1 {
					ratio = 0.1
				}
				w.resubmitAdjustCh <- &intervalAdjust{
					ratio: ratio,
					inc:   true,
				}
			}
			return atomic.LoadInt32(interrupt) == commitInterruptNewHead
		}
		// If we don't have enough gas for any further transactions then we're done
		if w.current.gasPool.Gas() < params.TxGas {
			log.Trace("Not enough gas for further transactions", "have", w.current.gasPool, "want", params.TxGas)
			break
		}
		// Retrieve the next transaction and abort if all done
		tx := txs.Peek()
		if tx == nil {
			break
		}
		// Error may be ignored here. The error has already been checked
		// during transaction acceptance is the transaction pool.
		//
		// We use the eip155 signer regardless of the current hf.
		from, _ := types.Sender(w.current.signer, tx)
		// Check whether the tx is replay protected. If we're not in the EIP155 hf
		// phase, start ignoring the sender until we do.
		if tx.Protected() && !w.config.IsEIP155(w.current.header.Number) && !tx.IsPrivate() {
			log.Trace("Ignoring reply protected transaction", "hash", tx.Hash(), "eip155", w.config.EIP155Block)

			txs.Pop()
			continue
		}
		// Start executing the transaction
		w.current.state.Prepare(tx.Hash(), common.Hash{}, w.current.tcount)
		w.current.privateState.Prepare(tx.Hash(), common.Hash{}, w.current.tcount)

		logs, err := w.commitTransaction(tx, coinbase)
		switch err {
		case core.ErrGasLimitReached:
			// Pop the current out-of-gas transaction without shifting in the next from the account
			log.Trace("Gas limit exceeded for current block", "sender", from)
			txs.Pop()

		case core.ErrNonceTooLow:
			// New head notification data race between the transaction pool and miner, shift
			log.Trace("Skipping transaction with low nonce", "sender", from, "nonce", tx.Nonce())
			txs.Shift()

		case core.ErrNonceTooHigh:
			// Reorg notification data race between the transaction pool and miner, skip account =
			log.Trace("Skipping account with hight nonce", "sender", from, "nonce", tx.Nonce())
			txs.Pop()

		case nil:
			// Everything ok, collect the logs and shift in the next transaction from the same account
			coalescedLogs = append(coalescedLogs, logs...)
			w.current.tcount++
			txs.Shift()

		default:
			// Strange error, discard the transaction and get the next in line (note, the
			// nonce-too-high clause will prevent us from executing in vain).
			log.Debug("Transaction failed, account skipped", "hash", tx.Hash(), "err", err)
			txs.Shift()
		}
	}

	if !w.isRunning() && len(coalescedLogs) > 0 {
		// We don't push the pendingLogsEvent while we are mining. The reason is that
		// when we are mining, the worker will regenerate a mining block every 3 seconds.
		// In order to avoid pushing the repeated pendingLog, we disable the pending log pushing.

		// make a copy, the state caches the logs and these logs get "upgraded" from pending to mined
		// logs by filling in the block hash when the block was mined by the local miner. This can
		// cause a race condition if a log was "upgraded" before the PendingLogsEvent is processed.
		cpy := make([]*types.Log, len(coalescedLogs))
		for i, l := range coalescedLogs {
			cpy[i] = new(types.Log)
			*cpy[i] = *l
		}
		go w.mux.Post(core.PendingLogsEvent{Logs: cpy})
	}
	// Notify resubmit loop to decrease resubmitting interval if current interval is larger
	// than the user-specified one.
	if interrupt != nil {
		w.resubmitAdjustCh <- &intervalAdjust{inc: false}
	}
	return false
}

// commitNewWork generates several new sealing tasks based on the parent block.
func (w *worker) commitNewWork(interrupt *int32, noempty bool, timestamp int64) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	tstart := time.Now()
	parent := w.chain.CurrentBlock()

	// @sourav, todo: double check whether this timing constraint is needed or not.
	if parent.Time().Cmp(new(big.Int).SetInt64(timestamp)) >= 0 {
		timestamp = parent.Time().Int64() + 1
	}
	// this will ensure we're not going off too far in the future
	if now := time.Now().Unix(); timestamp > now+1 {
		wait := time.Duration(timestamp-now) * time.Second
		log.Info("Mining too far in the future", "wait", common.PrettyDuration(wait))
		time.Sleep(wait)
	}

	num := parent.Number()
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     num.Add(num, common.Big1),
		RefNumber:  w.getRefNumber(),
		RefHash:    w.getRefHash(),
		Shard:      w.eth.MyShard(),
		GasLimit:   w.gasLimit,
		Extra:      w.extra,
		Time:       big.NewInt(timestamp),
	}

	// Only set the coinbase if our consensus engine is running (avoid spurious block rewards)
	if w.isRunning() {
		if w.coinbase == (common.Address{}) {
			log.Error("Refusing to mine without etherbase")
			return
		}
		header.Coinbase = w.coinbase
	}
	if err := w.engine.Prepare(w.chain, header); err != nil {
		log.Error("Failed to prepare header for mining", "err", err)
		return
	}
	// If we are care about TheDAO hard-fork check whether to override the extra-data or not
	if daoBlock := w.config.DAOForkBlock; daoBlock != nil {
		// Check whether the block is among the fork extra-override range
		limit := new(big.Int).Add(daoBlock, params.DAOForkExtraRange)
		if header.Number.Cmp(daoBlock) >= 0 && header.Number.Cmp(limit) < 0 {
			// Depending whether we support or oppose the fork, override differently
			if w.config.DAOForkSupport {
				header.Extra = common.CopyBytes(params.DAOForkBlockExtra)
			} else if bytes.Equal(header.Extra, params.DAOForkBlockExtra) {
				header.Extra = []byte{} // If miner opposes, don't let it use the reserved extra-data
			}
		}
	}
	// Could potentially happen if starting to mine in an odd state.
	err := w.makeCurrent(parent, header)
	if err != nil {
		log.Error("Failed to create mining context", "err", err)
		return
	}
	// Create the current work task and check any fork transitions needed
	env := w.current
	if w.config.DAOForkSupport && w.config.DAOForkBlock != nil && w.config.DAOForkBlock.Cmp(header.Number) == 0 {
		misc.ApplyDAOHardFork(env.state)
	}
	// Accumulate the uncles for the current block
	uncles := make([]*types.Header, 0, 2)
	commitUncles := func(blocks map[common.Hash]*types.Block) {
		// Clean up stale uncle blocks first
		for hash, uncle := range blocks {
			if uncle.NumberU64()+staleThreshold <= header.Number.Uint64() {
				delete(blocks, hash)
			}
		}
		for hash, uncle := range blocks {
			if len(uncles) == 2 {
				break
			}
			if err := w.commitUncle(env, uncle.Header()); err != nil {
				log.Trace("Possible uncle rejected", "hash", hash, "reason", err)
			} else {
				log.Debug("Committing new uncle to block", "hash", hash)
				uncles = append(uncles, uncle.Header())
			}
		}
	}
	// Prefer to locally generated uncle
	commitUncles(w.localUncles)
	commitUncles(w.remoteUncles)

	// // If the block is first block, then deploy all contracts
	if header.Number.Cmp(common.Big1) == 0 {
		if w.commitInitialContract(w.coinbase, interrupt) {
			return
		}
		w.commit(uncles, w.fullTaskHook, true, tstart)
		return
	}

	if !noempty {
		// Create an empty block based on temporary copied state for sealing in advance without waiting block
		// execution finished.
		w.commit(uncles, nil, false, tstart)
	}

	// Fill the block with all available pending transactions.
	pending, err := w.eth.TxPool().Pending()
	if err != nil {
		log.Error("Failed to fetch pending transactions", "err", err)
		return
	}
	/**
	// Short circuit if there is no available pending transactions
	if len(pending) == 0 {
		w.updateSnapshot()
		return
	}
	*/

	// Execute if data is avaialble for any new trasnactions
	w.cLockedAddr = make(map[common.Address]*types.CLock)
	w.unlockedAddr = make(map[common.Address]*types.CLock)

	pTxs := w.getPromotedTransactions()
	if w.commitCrossTransactions(pTxs, w.coinbase, interrupt) {
		return
	}

	// Produce status of new cross-shard tranactons while respecting
	// execution result of current block.
	start := parent.RefNumberU64() + 1
	end := w.getRefNumberU64()
	if end >= start {
		cTxs, aTxs := w.getNewTransactions(start, end) // commit, abort pair
		if w.commitNewTransactions(true, cTxs, w.coinbase, interrupt) {
			return
		}
		if w.commitNewTransactions(false, aTxs, w.coinbase, interrupt) {
			return
		}
	}

	// Split the pending transactions into locals and remotes
	localTxs, remoteTxs := make(map[common.Address]types.Transactions), pending
	for _, account := range w.eth.TxPool().Locals() {
		if txs := remoteTxs[account]; len(txs) > 0 {
			delete(remoteTxs, account)
			localTxs[account] = txs
		}
	}
	if len(localTxs) > 0 {
		txs := types.NewTransactionsByPriceAndNonce(w.current.signer, localTxs)
		if w.commitTransactions(txs, w.coinbase, interrupt) {
			return
		}
	}
	if len(remoteTxs) > 0 {
		txs := types.NewTransactionsByPriceAndNonce(w.current.signer, remoteTxs)
		if w.commitTransactions(txs, w.coinbase, interrupt) {
			return
		}
	}
	w.commit(uncles, w.fullTaskHook, true, tstart)
}

// commit runs any post-transaction state modifications, assembles the final block
// and commits new work if consensus engine is running.
func (w *worker) commit(uncles []*types.Header, interval func(), update bool, start time.Time) error {
	// Deep copy receipts here to avoid interaction between different tasks.
	receipts := make([]*types.Receipt, len(w.current.receipts))
	for i, l := range w.current.receipts {
		receipts[i] = new(types.Receipt)
		*receipts[i] = *l
	}

	privateReceipts := make([]*types.Receipt, len(w.current.privateReceipts))
	for i, l := range w.current.privateReceipts {
		privateReceipts[i] = new(types.Receipt)
		*privateReceipts[i] = *l
	}

	s := w.current.state.Copy()
	ps := w.current.privateState.Copy()
	block, err := w.engine.Finalize(w.chain, w.current.header, s, w.current.txs, uncles, w.current.receipts)
	if err != nil {
		return err
	}
	if w.isRunning() {
		if interval != nil {
			interval()
		}
		select {
		case w.taskCh <- &task{receipts: receipts, privateReceipts: privateReceipts, state: s, privateState: ps, block: block, createdAt: time.Now()}:
			w.unconfirmed.Shift(block.NumberU64() - 1)

			feesWei := new(big.Int)
			for i, tx := range block.Transactions() {
				feesWei.Add(feesWei, new(big.Int).Mul(new(big.Int).SetUint64(receipts[i].GasUsed), tx.GasPrice()))
			}
			feesEth := new(big.Float).Quo(new(big.Float).SetInt(feesWei), new(big.Float).SetInt(big.NewInt(params.Ether)))

			log.Info("Commit new mining work", "number", block.Number(), "sealhash", w.engine.SealHash(block.Header()), "root", block.Root(),
				"uncles", len(uncles), "txs", w.current.tcount, "gas", block.GasUsed(), "fees", feesEth, "elapsed", common.PrettyDuration(time.Since(start)))

		case <-w.exitCh:
			log.Info("Worker has exited")
		}
	}
	if update {
		w.updateSnapshot()
	}
	return nil
}
