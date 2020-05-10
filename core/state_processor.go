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

package core

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
)

// StateProcessor is a basic Processor, which takes care of transitioning
// state from one point to another.
//
// StateProcessor implements Processor.
type StateProcessor struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain
	engine consensus.Engine    // Consensus engine used for block rewards
}

// NewStateProcessor initialises a new StateProcessor.
func NewStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *StateProcessor {
	return &StateProcessor{
		config: config,
		bc:     bc,
		engine: engine,
	}
}

// Process processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb and applying any rewards to both
// the processor (coinbase) and any included uncles.
//
// Process returns the receipts and logs accumulated during the process and
// returns the amount of gas that was used in the process. If any of the
// transactions failed to execute due to insufficient gas it will return an error.
func (p *StateProcessor) Process(block *types.Block, statedb, privateState *state.StateDB, cfg vm.Config) (types.Receipts, types.Receipts, []*types.Log, uint64, error) {

	var (
		receipts types.Receipts
		usedGas  = new(uint64)
		header   = block.Header()
		allLogs  []*types.Log
		gp       = new(GasPool).AddGas(block.GasLimit())
		tcb      *types.TxControl

		privateReceipts types.Receipts
		txType          uint64
		ccount          = uint64(0)
		elemSize        = 32
		u64Offset       = 24
		croot           = block.Root()
		myshard         = p.bc.MyShard()
		tHash           common.Hash
	)
	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	// Iterate over and process the individual transactions
	for i, tx := range block.Transactions() {
		txType = tx.TxType()
		tHash = tx.Hash()
		statedb.Prepare(tHash, block.Hash(), i)
		privateState.Prepare(tHash, block.Hash(), i)

		snap := statedb.Snapshot()
		psnap := privateState.Snapshot()

		if txType == types.CrossShardLocal {
			p.bc.crossTxsMu.RLock()
			tcb = p.bc.pendingCrossTxs[tHash]
			p.bc.crossTxsMu.RUnlock()
		} else {
			tcb = nil
		}

		receipt, privateReceipt, _, err := ApplyTransaction(p.config, p.bc, nil, gp, tcb, p.bc.lockedAddr, nil, statedb, privateState, header, tx, usedGas, cfg)

		// Unlocking keys
		if txType == types.CrossShardLocal {
			tcb.TxControlMu.RLock()
			keyval := tcb.Keyval
			addrToShard := tcb.AddrToShard
			tcb.TxControlMu.RUnlock()

			for addr, keys := range keyval {
				if shard := addrToShard[addr]; shard == myshard {
					p.bc.lockedAddrMu.RLock()
					aclok := p.bc.lockedAddr[addr]
					p.bc.lockedAddrMu.RUnlock()

					aclok.ClockMu.Lock()
					for _, key := range keys.Keys {
						delete(aclok.Keys, key)
					}
					clockSize := len(aclok.Keys)
					aclok.ClockMu.Unlock()
					if clockSize == 0 {
						p.bc.lockedAddrMu.Lock()
						delete(p.bc.lockedAddr, addr)
						p.bc.lockedAddrMu.Unlock()
					}
				}
			}
			p.bc.promCrossMu.Lock()
			delete(p.bc.promCrossTxs, tHash)
			p.bc.promCrossMu.Unlock()

			log.Info("Executed cross-shard transaction", "thash", tHash, "bn", block.NumberU64())

		} else if txType == types.LocalDecision {
			index := 4
			data := tx.Data()
			index += elemSize
			tHash := common.BytesToHash(data[index : index+elemSize])
			index += elemSize
			status := binary.BigEndian.Uint64(data[index+u64Offset : index+elemSize])
			decision := status == uint64(1)

			log.Info("@cs Local Decision for", "th", tHash, "status", decision, "bn", block.NumberU64(), "sTh", tx.Hash())
			if decision {
				p.bc.crossTxsMu.RLock()
				tcb, tok := p.bc.pendingCrossTxs[tHash]
				if !tok {
					p.bc.crossTxsMu.RUnlock()
					continue
				}
				p.bc.crossTxsMu.RUnlock()

				commit := &types.TCommit{TxHash: tHash, Shard: myshard, Status: decision, StateRoot: croot}
				tcb.AddTCommit(commit)
				tcb.UpdateLocalStatus(myshard)

				tcb.TxControlMu.RLock()
				keyVal := tcb.Keyval
				addrToShard := tcb.AddrToShard
				tcb.TxControlMu.RUnlock()

				for addr, ckeys := range keyVal {
					if shard := addrToShard[addr]; shard == myshard {
						p.bc.lockedAddrMu.Lock()
						if _, ok := p.bc.lockedAddr[addr]; !ok {
							p.bc.lockedAddr[addr] = types.NewCLock(addr)
						}
						alock := p.bc.lockedAddr[addr]
						p.bc.lockedAddrMu.Unlock()

						alock.ClockMu.Lock()
						for _, key := range ckeys.Keys {
							alock.Keys[key] = false
						}
						alock.ClockMu.Unlock()
					}
				}

				// Keeping a marker that the transaction locked additional values
				p.bc.thLockedMu.Lock()
				if _, thok := p.bc.thLocked[tHash]; !thok {
					p.bc.thLocked[tHash] = true
				}
				p.bc.thLockedMu.Unlock()
			}
			/**
			else {
				p.bc.crossTxsMu.Lock()
				if _, tok := p.bc.pendingCrossTxs[tHash]; tok {
					delete(p.bc.pendingCrossTxs, tHash)
				}
				p.bc.crossTxsMu.Unlock()
			}
			*/
			ccount++
		}

		if tx.TxType() == types.CrossShardLocal && err != nil {
			statedb.RevertToSnapshot(snap)
			privateState.RevertToSnapshot(psnap)
			log.Warn("Skipping transaction", "thash", tx.Hash(), "from", tx.From(), "error", err)
			continue
		}
		if err != nil {
			return nil, nil, nil, 0, err
		}
		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)

		// if the private receipt is nil this means the tx was public
		// and we do not need to apply the additional logic.
		if privateReceipt != nil {
			privateReceipts = append(privateReceipts, privateReceipt)
			allLogs = append(allLogs, privateReceipt.Logs...)
		}
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	p.engine.Finalize(p.bc, header, statedb, block.Transactions(), block.Uncles(), receipts)
	p.bc.AddCount(ccount)
	return receipts, privateReceipts, allLogs, *usedGas, nil
}

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyTransaction(config *params.ChainConfig, bc *BlockChain, author *common.Address, gp *GasPool, tcb *types.TxControl, gLockedAddr, cUnlockedAddr map[common.Address]*types.CLock, statedb, privateState *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64, cfg vm.Config) (*types.Receipt, *types.Receipt, uint64, error) {
	if !config.IsQuorum || !tx.IsPrivate() {
		privateState = statedb
	}

	if config.IsQuorum && tx.GasPrice() != nil && tx.GasPrice().Cmp(common.Big0) > 0 {
		return nil, nil, 0, ErrInvalidGasPrice
	}

	// Updating the address of the transaction
	if tx.TxType() == types.TxnStatus {
		commitAddress := bc.CommitAddress()
		tx.SetRecipient(&commitAddress)
	}
	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	if err != nil {
		return nil, nil, 0, err
	}
	// Create a new context to be used in the EVM environment
	context := NewEVMContext(msg, header, bc, author)
	// Create a new environment which holds all relevant information
	// about the transaction and calling mechanisms.
	vmenv := vm.NewEVM(context, tcb, gLockedAddr, cUnlockedAddr, statedb, privateState, config, cfg)

	// Apply the transaction to the current state (included in the env)
	_, gas, failed, err := ApplyMessage(vmenv, msg, gp)
	if err != nil {
		return nil, nil, 0, err
	}

	// Update the state with pending changes
	var root []byte
	if config.IsByzantium(header.Number) {
		statedb.Finalise(true)
	} else {
		root = statedb.IntermediateRoot(config.IsEIP158(header.Number)).Bytes()
	}
	*usedGas += gas

	// If this is a private transaction, the public receipt should always
	// indicate success.
	publicFailed := !(config.IsQuorum && tx.IsPrivate()) && failed

	// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx
	// based on the eip phase, we're passing wether the root touch-delete accounts.
	receipt := types.NewReceipt(root, publicFailed, *usedGas)
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = gas
	// if the transaction created a contract, store the creation address in the receipt.
	if msg.To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(vmenv.Context.Origin, tx.Nonce())
	}
	// Set the receipt logs and create a bloom for filtering
	receipt.Logs = statedb.GetLogs(tx.Hash())
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})

	var privateReceipt *types.Receipt
	if config.IsQuorum && tx.IsPrivate() {
		var privateRoot []byte
		if config.IsByzantium(header.Number) {
			privateState.Finalise(false)
		} else {
			privateRoot = privateState.IntermediateRoot(config.IsEIP158(header.Number)).Bytes()
		}
		privateReceipt = types.NewReceipt(privateRoot, failed, *usedGas)
		privateReceipt.TxHash = tx.Hash()
		privateReceipt.GasUsed = gas
		if msg.To() == nil {
			privateReceipt.ContractAddress = crypto.CreateAddress(vmenv.Context.Origin, tx.Nonce())
		}

		privateReceipt.Logs = privateState.GetLogs(tx.Hash())
		privateReceipt.Bloom = types.CreateBloom(types.Receipts{privateReceipt})
	}

	return receipt, privateReceipt, gas, err
}
