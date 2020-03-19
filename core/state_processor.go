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
	"encoding/hex"
	"math/big"

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

		crossTxs        []*types.CrossTx
		privateReceipts types.Receipts
	)
	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	// Iterate over and process the individual transactions
	for i, tx := range block.Transactions() {
		statedb.Prepare(tx.Hash(), block.Hash(), i)
		privateState.Prepare(tx.Hash(), block.Hash(), i)

		receipt, privateReceipt, _, err := ApplyTransaction(p.config, p.bc, nil, gp, statedb, privateState, header, tx, usedGas, cfg)
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

		myshard := p.bc.MyShard()
		if tx.TxType() == types.CrossShard && myshard > uint64(0) {

			data := tx.Data()[4:]
			shardsInvolved, involved := DecodeCrossTx(myshard, data)
			if involved {
				elemSize := 32
				numShards := len(shardsInvolved)
				startIndex := (2+1+numShards)*elemSize + elemSize // Last 32 bytes to avoid string length
				crossTx := ParseCrossTxData(uint16(numShards), data[2+startIndex:])
				crossTx.BlockNum = block.Number()
				crossTxs = append(crossTxs, crossTx)
				log.Info("New cross shard transaction added!", "bn", block.NumberU64(), "shards", shardsInvolved)
			} else {
				log.Debug("Local shard not involved in cross shard transaction", "hash", tx.Hash())
			}
		}
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	p.engine.Finalize(p.bc, header, statedb, block.Transactions(), block.Uncles(), receipts)
	return receipts, privateReceipts, allLogs, *usedGas, nil
}

/**
	Num shard: 2 (2 bytes)
		Shard Id: 2 (2 bytes)
			Num contracts: 1 (2 bytes)
			Contract Id: 0xd5A39372C999DEEfAC30e44861D55A5F7f95a60F (20 bytes)
				Num Keys: 10 (2 bytes)
				Key 1: (32 bytes)
				Key 1: (32 bytes)
					...
				Key k: (32 bytes)
			Contract Id: ... (32 bytes)
				Num Keys: 10 (2 bytes)
				Key 1: (32 bytes)
				Key 1: (32 bytes)
					...
				Key k: (32 bytes)

				...
			Contract Id: ... (32 bytes)
				Num Keys: 10 (2 bytes)
				Key 1: (32 bytes)
				Key 1: (32 bytes)
					...
				Key k: (32 bytes)
	Sender Id: ... (20 bytes)
	Nonce: ... (4 bytes)
	Value: ... (4 bytes)
	Receiver: ... (32 bytes)
	GasLimit: ... (8 bytes)
	GasPrice: ... (8 bytes)
	Function sign: ... (4 bytes)
	Function params: ... (a*32 bytes) # a is the number of paramers
**/

// ParseCrossTxData parsed data
func ParseCrossTxData(numShard uint16, data []byte) *types.CrossTx {
	startIndex := uint16(0)
	elemSize := uint16(32)
	addrSize := uint16(20)
	crossTx := &types.CrossTx{
		Shards:       []uint64{},
		AllContracts: make(map[uint64][]types.CKeys),
	}
	for i := uint16(0); i < numShard; i++ {
		shard := binary.BigEndian.Uint16(data[startIndex : startIndex+2])

		crossTx.Shards = append(crossTx.Shards, uint64(shard))
		crossTx.AllContracts[uint64(shard)] = []types.CKeys{}
		startIndex = startIndex + 2
		numContracts := binary.BigEndian.Uint16(data[startIndex : startIndex+2])
		startIndex = startIndex + 2

		if numContracts > 0 {
			for j := uint16(0); j < numContracts; j++ {
				addr := common.BytesToAddress(data[startIndex : startIndex+addrSize])
				startIndex = startIndex + addrSize
				numKeys := binary.BigEndian.Uint16(data[startIndex : startIndex+2])
				startIndex = startIndex + 2
				cKeys := &types.CKeys{Addr: addr, Keys: []uint64{}}
				for k := uint16(0); k < numKeys; k++ {
					key := binary.BigEndian.Uint64(data[startIndex+24 : startIndex+elemSize])
					cKeys.Keys = append(cKeys.Keys, key)
					startIndex = startIndex + elemSize
				}
				crossTx.AllContracts[uint64(shard)] = append(crossTx.AllContracts[uint64(shard)], *cKeys)
			}
		}
	}

	sender := common.BytesToAddress(data[startIndex : startIndex+addrSize])
	startIndex = startIndex + addrSize
	nonce := binary.BigEndian.Uint32(data[startIndex : startIndex+uint16(4)])
	startIndex = startIndex + uint16(4)
	value := binary.BigEndian.Uint32(data[startIndex : startIndex+uint16(4)])
	startIndex = startIndex + uint16(4)
	receiver := common.BytesToAddress(data[startIndex : startIndex+addrSize])
	startIndex = startIndex + addrSize
	gasLimit := binary.BigEndian.Uint64(data[startIndex : startIndex+uint16(8)])
	startIndex = startIndex + uint16(8)
	gasPrice := binary.BigEndian.Uint64(data[startIndex : startIndex+uint16(8)])
	startIndex = startIndex + uint16(8)
	funcSig := hex.EncodeToString(data[startIndex : startIndex+uint16(4)])
	startIndex = startIndex + uint16(4)
	log.Info("Cross shard Transaction information", "from", sender, "to", receiver, "nonce", nonce, "value", value, "function", funcSig, "gl", gasLimit, "gp", gasPrice, "params", hex.EncodeToString(data[startIndex:]))
	tx := types.NewTransaction(types.CrossShardLocal, uint64(nonce), uint64(0), receiver, big.NewInt(int64(value)), gasLimit, big.NewInt(int64(gasPrice)), data[startIndex:])
	tx.SetFrom(sender)
	crossTx.Tx = tx
	return crossTx
}

// DecodeCrossTx extracts shards
func DecodeCrossTx(myshard uint64, data []byte) ([]uint64, bool) {
	elemSize := uint64(32)
	lenData := data[2*elemSize+elemSize-8 : 3*elemSize]
	length := binary.BigEndian.Uint64(lenData)
	startIndex := 3 * elemSize
	var (
		involved = false
		shards   []uint64
	)
	for i := uint64(0); i < length; i++ {
		shardData := data[startIndex+i*elemSize+24 : startIndex+(i+1)*elemSize]
		shard := binary.BigEndian.Uint64(shardData)
		if shard == myshard {
			involved = true
		}
		shards = append(shards, shard)
	}
	return shards, involved
}

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyTransaction(config *params.ChainConfig, bc *BlockChain, author *common.Address, gp *GasPool, statedb, privateState *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64, cfg vm.Config) (*types.Receipt, *types.Receipt, uint64, error) {
	if !config.IsQuorum || !tx.IsPrivate() {
		privateState = statedb
	}

	if config.IsQuorum && tx.GasPrice() != nil && tx.GasPrice().Cmp(common.Big0) > 0 {
		return nil, nil, 0, ErrInvalidGasPrice
	}

	// Updating the address of the transaction
	if tx.TxType() == types.StateCommit {
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
	vmenv := vm.NewEVM(context, statedb, privateState, config, cfg)

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
			privateState.Finalise(true)
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
