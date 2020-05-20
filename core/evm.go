// Copyright 2016 The go-ethereum Authors
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
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
)

// ChainContext supports retrieving headers and consensus parameters from the
// current blockchain to be used during transaction processing.
type ChainContext interface {
	// Engine retrieves the chain's consensus engine.
	Engine() consensus.Engine

	// GetHeader returns the hash corresponding to their hash.
	GetHeader(common.Hash, uint64) *types.Header
}

// NewEVMContext creates a new context for use in the EVM.
func NewEVMContext(msg Message, header *types.Header, chain ChainContext, author *common.Address) vm.Context {
	// If we don't have an explicit author (i.e. not mining), extract from the header
	var beneficiary common.Address
	if author == nil {
		beneficiary, _ = chain.Engine().Author(header) // Ignore error, we're past header validation
	} else {
		beneficiary = *author
	}
	return vm.Context{
		CanTransfer: CanTransfer,
		Transfer:    Transfer,
		GetHash:     GetHashFn(header, chain),
		Origin:      msg.From(),
		Coinbase:    beneficiary,
		BlockNumber: new(big.Int).Set(header.Number),
		Time:        new(big.Int).Set(header.Time),
		Difficulty:  new(big.Int).Set(header.Difficulty),
		GasLimit:    header.GasLimit,
		GasPrice:    new(big.Int).Set(msg.GasPrice()),
		Shard:       header.Shard,
	}
}

// GetHashFn returns a GetHashFunc which retrieves header hashes by number
func GetHashFn(ref *types.Header, chain ChainContext) func(n uint64) common.Hash {
	var cache map[uint64]common.Hash

	return func(n uint64) common.Hash {
		// If there's no hash cache yet, make one
		if cache == nil {
			cache = map[uint64]common.Hash{
				ref.Number.Uint64() - 1: ref.ParentHash,
			}
		}
		// Try to fulfill the request from the cache
		if hash, ok := cache[n]; ok {
			return hash
		}
		// Not cached, iterate the blocks and cache the hashes
		for header := chain.GetHeader(ref.ParentHash, ref.Number.Uint64()-1); header != nil; header = chain.GetHeader(header.ParentHash, header.Number.Uint64()-1) {
			cache[header.Number.Uint64()-1] = header.ParentHash
			if n == header.Number.Uint64()-1 {
				return header.ParentHash
			}
		}
		return common.Hash{}
	}
}

// CanTransfer checks whether there are enough funds in the address' account to make a transfer.
// This does not take the necessary gas in to account to make the transfer valid.
func CanTransfer(tcb *types.TxControl, gLocked, cUnocked *types.RWLock, bshard uint64, db vm.StateDB, addr common.Address, amount *big.Int) bool {
	var balance *big.Int
	if tcb != nil {
		tcb.TxControlMu.RLock()
		defer tcb.TxControlMu.RUnlock()
		if shard, sok := tcb.AddrToShard[addr]; sok {
			if shard != bshard {
				balance = new(big.Int).SetUint64(tcb.Values[addr].Balance)
			} else {
				balance = db.GetBalance(addr)
			}
			return balance.Cmp(amount) >= 0
		}
		return false
	}
	// @sourav, todo: decide whether to put these checks or
	// not. We can decide to ignore this check if needed!
	// We will have to anyway check during data access.

	// Check if the locks are already held for any keys
	// avoid executing new transactions involving the
	// same user.
	/*
		if gLock, gaok := gLocked.Locks[addr]; gaok {
			if _, uaok := cUnlocked.Locks[addr]; !uaok {
				log.Info("@cs, address locked in global lock!", "addr", addr)
				return false
			}
			uLock := cUnlocked.Locks[addr]
			for gKey := range gLock.Keys {
				if _, kok := uLock.Keys[gKey]; !kok {
					log.Info("@cs, key locked for address", "addr", addr)
					return false
				}
			}
		}
	*/
	return db.GetBalance(addr).Cmp(amount) >= 0
}

// Transfer subtracts amount from sender and adds amount to recipient using the given Db
func Transfer(bshard uint64, tcb *types.TxControl, tcbChanges map[common.Address]*types.CData, db vm.StateDB, sender, recipient common.Address, amount *big.Int) {
	if tcb != nil {
		tcb.TxControlMu.RLock()
		tcb.TxControlMu.RUnlock()
		sshard := tcb.AddrToShard[sender]
		if sshard != bshard {
			if _, ok := tcbChanges[sender]; !ok {
				vals := tcb.Values[sender]
				if vals != nil {
					tcbChanges[sender] = &types.CData{
						Addr:    sender,
						Balance: vals.Balance,
						Nonce:   vals.Nonce,
						Data:    make(map[common.Hash]common.Hash),
					}
				} else {
					tcbChanges[recipient] = &types.CData{
						Addr:    recipient,
						Balance: db.GetBalance(recipient).Uint64(),
						Nonce:   db.GetNonce(recipient),
						Data:    make(map[common.Hash]common.Hash),
					}
				}
			}
			tcbChanges[sender].Balance = tcbChanges[sender].Balance - amount.Uint64()
		} else {
			db.SubBalance(sender, amount)
		}

		rshard := tcb.AddrToShard[recipient]
		if rshard != bshard {
			if _, ok := tcbChanges[recipient]; !ok {
				vals := tcb.Values[recipient]
				if vals != nil {
					tcbChanges[recipient] = &types.CData{
						Addr:    recipient,
						Balance: vals.Balance,
						Nonce:   vals.Nonce,
						Data:    make(map[common.Hash]common.Hash),
					}
				} else {
					tcbChanges[recipient] = &types.CData{
						Addr:    recipient,
						Balance: db.GetBalance(recipient).Uint64(),
						Nonce:   db.GetNonce(recipient),
						Data:    make(map[common.Hash]common.Hash),
					}
				}
			}
			tcbChanges[recipient].Balance = tcbChanges[recipient].Balance + amount.Uint64()
		} else {
			db.AddBalance(recipient, amount)
		}
	} else {
		db.SubBalance(sender, amount)
		db.AddBalance(recipient, amount)
	}
}
