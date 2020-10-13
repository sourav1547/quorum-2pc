// Copyright 2014 The go-ethereum Authors
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

package types

import (
	"container/heap"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math/big"
	"sync"
	"sync/atomic"

	fmt "fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

//go:generate gencodec -type txdata -field-override txdataMarshaling -out gen_tx_json.go

// Various transaction Type
const (
	TxnStatus       = uint64(0) // Commitment status of a cross-shard Transaction
	IntraShard      = uint64(1) // Intra Shard Transaction
	CrossShard      = uint64(2) // Cross Shard Transaction
	ContractInit    = uint64(3) // Initializing Contracts
	CrossShardLocal = uint64(4) // Cross shard transaction for local execution.
	LocalDecision   = uint64(5)
	Acknowledgement = uint64(6)
	Others          = uint64(7)
)

var (
	ErrInvalidSig = errors.New("invalid transaction v, r, s values")
)

// deriveSigner makes a *best* guess about which signer to use.
func deriveSigner(V *big.Int) Signer {
	// joel: this is one of the two places we used a wrong signer to print txes
	if V.Sign() != 0 && isProtectedV(V) {
		return NewEIP155Signer(deriveChainId(V))
	} else if isPrivate(V) {
		return QuorumPrivateTxSigner{}
	} else {
		return HomesteadSigner{}
	}
}

type Transaction struct {
	data txdata
	// caches
	hash atomic.Value
	size atomic.Value
	from atomic.Value
}

type txdata struct {
	TxType       uint64          `json:"txType"   gencodec:"required"`
	AccountNonce uint64          `json:"nonce"    gencodec:"required"`
	Shard        uint64          `json:"shard"	  gencoded:"required"`
	Price        *big.Int        `json:"gasPrice" gencodec:"required"`
	GasLimit     uint64          `json:"gas"      gencodec:"required"`
	Recipient    *common.Address `json:"to"       rlp:"nil"` // nil means contract creation
	Sender       *common.Address `json:"sender" rlp:"nil"`
	Amount       *big.Int        `json:"value"    gencodec:"required"`
	Payload      []byte          `json:"input"    gencodec:"required"`

	// Signature values
	V *big.Int `json:"v" gencodec:"required"`
	R *big.Int `json:"r" gencodec:"required"`
	S *big.Int `json:"s" gencodec:"required"`

	// This is only used when marshaling to JSON.
	Hash *common.Hash `json:"hash" rlp:"-"`
}

type txdataMarshaling struct {
	TxType       hexutil.Uint64
	AccountNonce hexutil.Uint64
	Shard        hexutil.Uint64
	Price        *hexutil.Big
	GasLimit     hexutil.Uint64
	Amount       *hexutil.Big
	Payload      hexutil.Bytes
	V            *hexutil.Big
	R            *hexutil.Big
	S            *hexutil.Big
}

// RefAddress Returns the refAddress
func RefAddress() common.Address {
	seed := "6462C73A8D4913910C5AAA748EA82CD67EB4B73D"
	refAddress := new(big.Int)
	refAddress, _ = refAddress.SetString(seed, 16)
	return common.BigToAddress(refAddress)
}

// ShardAddress returns the unique address of each shard!
func ShardAddress(shard uint64) common.Address {
	seed := "6462C73A8D4913910C5AAA748EA82CD67EB4B73D"
	refAddress := new(big.Int)
	refAddress, _ = refAddress.SetString(seed, 16)
	addr := new(big.Int).SetUint64(shard)
	addr.Add(addr, refAddress)
	return common.BigToAddress(addr)
}

func NewTransaction(txType, nonce uint64, shard uint64, to common.Address, amount *big.Int, gasLimit uint64, gasPrice *big.Int, data []byte) *Transaction {
	return newTransaction(txType, nonce, shard, &to, amount, gasLimit, gasPrice, data)
}

// NewContractCreation creates a new contract
// func NewContractCreation(nonce uint64, amount *big.Int, gasLimit uint64, gasPrice *big.Int, data []byte) *Transaction {
// 	return newTransaction(uint64(0), nonce, uint64(0), nil, amount, gasLimit, gasPrice, data)
// }
func NewContractCreation(txType, nonce uint64, shard uint64, amount *big.Int, gasLimit uint64, gasPrice *big.Int, data []byte) *Transaction {
	return newTransaction(txType, nonce, uint64(0), nil, amount, gasLimit, gasPrice, data)
}

// NewCrossTransaction creates a new cross-shard transaction
func NewCrossTransaction(txType, nonce uint64, shard uint64, to, sender common.Address, amount *big.Int, gasLimit uint64, gasPrice *big.Int, data []byte) *Transaction {
	return newCrossTransaction(txType, nonce, shard, &to, &sender, amount, gasLimit, gasPrice, data)
}

func newCrossTransaction(txType, nonce uint64, shard uint64, to, sender *common.Address, amount *big.Int, gasLimit uint64, gasPrice *big.Int, data []byte) *Transaction {
	if len(data) > 0 {
		data = common.CopyBytes(data)
	}
	d := txdata{
		TxType:       txType,
		AccountNonce: nonce,
		Shard:        shard,
		Recipient:    to,
		Sender:       sender,
		Payload:      data,
		Amount:       new(big.Int),
		GasLimit:     gasLimit,
		Price:        new(big.Int),
		V:            new(big.Int),
		R:            new(big.Int),
		S:            new(big.Int),
	}
	if amount != nil {
		d.Amount.Set(amount)
	}
	if gasPrice != nil {
		d.Price.Set(gasPrice)
	}
	return &Transaction{data: d}
}

func newTransaction(txType, nonce uint64, shard uint64, to *common.Address, amount *big.Int, gasLimit uint64, gasPrice *big.Int, data []byte) *Transaction {
	if len(data) > 0 {
		data = common.CopyBytes(data)
	}
	d := txdata{
		TxType:       txType,
		AccountNonce: nonce,
		Shard:        shard,
		Recipient:    to,
		Payload:      data,
		Amount:       new(big.Int),
		GasLimit:     gasLimit,
		Price:        new(big.Int),
		V:            new(big.Int),
		R:            new(big.Int),
		S:            new(big.Int),
	}
	if amount != nil {
		d.Amount.Set(amount)
	}
	if gasPrice != nil {
		d.Price.Set(gasPrice)
	}

	return &Transaction{data: d}
}

// SetRecipient updates the recipient of a transaction
func (tx *Transaction) SetRecipient(to *common.Address) {
	tx.data.Recipient = to
}

// ChainId returns which chain id this transaction was signed for (if at all)
func (tx *Transaction) ChainId() *big.Int {
	return deriveChainId(tx.data.V)
}

// Protected returns whether the transaction is protected from replay protection.
func (tx *Transaction) Protected() bool {
	return isProtectedV(tx.data.V)
}

func isProtectedV(V *big.Int) bool {
	if V.BitLen() <= 8 {
		v := V.Uint64()
		// 27 / 28 are pre eip 155 -- ie unprotected.
		return !(v == 27 || v == 28)
	}
	// anything not 27 or 28 is considered protected
	return true
}

// EncodeRLP implements rlp.Encoder
func (tx *Transaction) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &tx.data)
}

// DecodeRLP implements rlp.Decoder
func (tx *Transaction) DecodeRLP(s *rlp.Stream) error {
	_, size, _ := s.Kind()
	err := s.Decode(&tx.data)
	if err == nil {
		tx.size.Store(common.StorageSize(rlp.ListSize(size)))
	}

	return err
}

// MarshalJSON encodes the web3 RPC transaction format.
func (tx *Transaction) MarshalJSON() ([]byte, error) {
	hash := tx.Hash()
	data := tx.data
	data.Hash = &hash
	return data.MarshalJSON()
}

// UnmarshalJSON decodes the web3 RPC transaction format.
func (tx *Transaction) UnmarshalJSON(input []byte) error {
	var dec txdata
	if err := dec.UnmarshalJSON(input); err != nil {
		return err
	}

	withSignature := dec.V.Sign() != 0 || dec.R.Sign() != 0 || dec.S.Sign() != 0
	if withSignature {
		var V byte
		if isProtectedV(dec.V) {
			chainID := deriveChainId(dec.V).Uint64()
			V = byte(dec.V.Uint64() - 35 - 2*chainID)
		} else {
			V = byte(dec.V.Uint64() - 27)
		}
		if !crypto.ValidateSignatureValues(V, dec.R, dec.S, false) {
			return ErrInvalidSig
		}
	}

	*tx = Transaction{data: dec}
	return nil
}

func (tx *Transaction) Data() []byte       { return common.CopyBytes(tx.data.Payload) }
func (tx *Transaction) TxData() txdata     { return tx.data }
func (tx *Transaction) Gas() uint64        { return tx.data.GasLimit }
func (tx *Transaction) GasPrice() *big.Int { return new(big.Int).Set(tx.data.Price) }
func (tx *Transaction) Value() *big.Int    { return new(big.Int).Set(tx.data.Amount) }
func (tx *Transaction) Nonce() uint64      { return tx.data.AccountNonce }
func (tx *Transaction) TxType() uint64     { return tx.data.TxType }
func (tx *Transaction) Shard() uint64      { return tx.data.Shard }
func (tx *Transaction) CheckNonce() bool   { return true }

// To returns the recipient address of the transaction.
// It returns nil if the transaction is a contract creation.
func (tx *Transaction) To() *common.Address {
	if tx.data.Recipient == nil {
		return nil
	}
	to := *tx.data.Recipient
	return &to
}

// From returns the address of the sender!
func (tx *Transaction) From() common.Address {
	if tx.TxType() == CrossShardLocal {
		return *tx.data.Sender
	}
	signer := deriveSigner(tx.data.V)
	if from, err := Sender(signer, tx); err == nil {
		return from
	}
	return common.Address{}
}

// Hash hashes the RLP encoding of tx.
// It uniquely identifies the transaction.
func (tx *Transaction) Hash() common.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return hash.(common.Hash)
	}
	v := rlpHash(tx)
	tx.hash.Store(v)
	return v
}

// Size returns the true RLP encoded storage size of the transaction, either by
// encoding and returning it, or returning a previsouly cached value.
func (tx *Transaction) Size() common.StorageSize {
	if size := tx.size.Load(); size != nil {
		return size.(common.StorageSize)
	}
	c := writeCounter(0)
	rlp.Encode(&c, &tx.data)
	tx.size.Store(common.StorageSize(c))
	return common.StorageSize(c)
}

// AsMessage returns the transaction as a core.Message.
//
// AsMessage requires a signer to derive the sender.
//
// XXX Rename message to something less arbitrary?
func (tx *Transaction) AsMessage(s Signer) (Message, error) {
	msg := Message{
		txType:     tx.data.TxType,
		nonce:      tx.data.AccountNonce,
		shard:      tx.data.Shard,
		gasLimit:   tx.data.GasLimit,
		gasPrice:   new(big.Int).Set(tx.data.Price),
		to:         tx.data.Recipient,
		amount:     tx.data.Amount,
		data:       tx.data.Payload,
		checkNonce: true,
		isPrivate:  tx.IsPrivate(),
	}

	var err error
	msg.from, err = Sender(s, tx)
	return msg, err
}

// SetFrom stores senders address
func (tx *Transaction) SetFrom(signer Signer, addr common.Address) {
	tx.from.Store(sigCache{signer: signer, from: addr})
}

// WithSignature returns a new transaction with the given signature.
// This signature needs to be formatted as described in the yellow paper (v+27).
func (tx *Transaction) WithSignature(signer Signer, sig []byte) (*Transaction, error) {
	r, s, v, err := signer.SignatureValues(tx, sig)
	if err != nil {
		return nil, err
	}
	cpy := &Transaction{data: tx.data}
	cpy.data.R, cpy.data.S, cpy.data.V = r, s, v
	return cpy, nil
}

// Cost returns amount + gasprice * gaslimit.
func (tx *Transaction) Cost() *big.Int {
	total := new(big.Int).Mul(tx.data.Price, new(big.Int).SetUint64(tx.data.GasLimit))
	total.Add(total, tx.data.Amount)
	return total
}

func (tx *Transaction) RawSignatureValues() (*big.Int, *big.Int, *big.Int) {
	return tx.data.V, tx.data.R, tx.data.S
}

func (tx *Transaction) String() string {
	var from, to string
	if tx.data.V != nil {
		// make a best guess about the signer and use that to derive
		// the sender.
		signer := deriveSigner(tx.data.V)
		if f, err := Sender(signer, tx); err != nil { // derive but don't cache
			from = "[invalid sender: invalid sig]"
		} else {
			from = fmt.Sprintf("%x", f[:])
		}
	} else {
		from = "[invalid sender: nil V field]"
	}

	if tx.data.Recipient == nil {
		to = "[contract creation]"
	} else {
		to = fmt.Sprintf("%x", tx.data.Recipient[:])
	}
	enc, _ := rlp.EncodeToBytes(&tx.data)
	return fmt.Sprintf(`
	TX(%x)
	Contract: %v
	From:     %s
	To:       %s
	Nonce:    %v
	GasPrice: %#x
	GasLimit  %#x
	Value:    %#x
	Data:     0x%x
	V:        %#x
	R:        %#x
	S:        %#x
	Hex:      %x
`,
		tx.Hash(),
		tx.data.Recipient == nil,
		from,
		to,
		tx.data.AccountNonce,
		tx.data.Price,
		tx.data.GasLimit,
		tx.data.Amount,
		tx.data.Payload,
		tx.data.V,
		tx.data.R,
		tx.data.S,
		enc,
	)
}

// Transactions is a Transaction slice type for basic sorting.
type Transactions []*Transaction

// Len returns the length of s.
func (s Transactions) Len() int { return len(s) }

// Swap swaps the i'th and the j'th element in s.
func (s Transactions) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// GetRlp implements Rlpable and returns the i'th element of s in rlp.
func (s Transactions) GetRlp(i int) []byte {
	enc, _ := rlp.EncodeToBytes(s[i])
	return enc
}

// TxDifference returns a new set which is the difference between a and b.
func TxDifference(a, b Transactions) Transactions {
	keep := make(Transactions, 0, len(a))

	remove := make(map[common.Hash]struct{})
	for _, tx := range b {
		remove[tx.Hash()] = struct{}{}
	}

	for _, tx := range a {
		if _, ok := remove[tx.Hash()]; !ok {
			keep = append(keep, tx)
		}
	}

	return keep
}

// TxByNonce implements the sort interface to allow sorting a list of transactions
// by their nonces. This is usually only useful for sorting transactions from a
// single account, otherwise a nonce comparison doesn't make much sense.
type TxByNonce Transactions

func (s TxByNonce) Len() int           { return len(s) }
func (s TxByNonce) Less(i, j int) bool { return s[i].data.AccountNonce < s[j].data.AccountNonce }
func (s TxByNonce) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// TxByPrice implements both the sort and the heap interface, making it useful
// for all at once sorting as well as individually adding and removing elements.
type TxByPrice Transactions

func (s TxByPrice) Len() int           { return len(s) }
func (s TxByPrice) Less(i, j int) bool { return s[i].data.Price.Cmp(s[j].data.Price) > 0 }
func (s TxByPrice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s *TxByPrice) Push(x interface{}) {
	*s = append(*s, x.(*Transaction))
}

func (s *TxByPrice) Pop() interface{} {
	old := *s
	n := len(old)
	x := old[n-1]
	*s = old[0 : n-1]
	return x
}

// TransactionsByPriceAndNonce represents a set of transactions that can return
// transactions in a profit-maximizing sorted order, while supporting removing
// entire batches of transactions for non-executable accounts.
type TransactionsByPriceAndNonce struct {
	txs    map[common.Address]Transactions // Per account nonce-sorted list of transactions
	heads  TxByPrice                       // Next transaction for each unique account (price heap)
	signer Signer                          // Signer for the set of transactions
}

// NewTransactionsByPriceAndNonce creates a transaction set that can retrieve
// price sorted transactions in a nonce-honouring way.
//
// Note, the input map is reowned so the caller should not interact any more with
// if after providing it to the constructor.
func NewTransactionsByPriceAndNonce(signer Signer, txs map[common.Address]Transactions) *TransactionsByPriceAndNonce {
	// Initialize a price based heap with the head transactions
	heads := make(TxByPrice, 0, len(txs))
	for from, accTxs := range txs {
		// Ensure the sender address is from the signer
		acc, err := Sender(signer, accTxs[0])
		if err == nil {
			heads = append(heads, accTxs[0])
			txs[acc] = accTxs[1:]
		} else {
			log.Info("Failed to recovered sender address, this transaction is skipped", "from", from, "nonce", accTxs[0].data.AccountNonce, "err", err)
		}
		if from != acc {
			log.Info("Deleting transaction ", "from", from, "acc", acc)
			delete(txs, from)
		}
	}
	heap.Init(&heads)

	// Assemble and return the transaction set
	return &TransactionsByPriceAndNonce{
		txs:    txs,
		heads:  heads,
		signer: signer,
	}
}

// Peek returns the next transaction by price.
func (t *TransactionsByPriceAndNonce) Peek() *Transaction {
	if len(t.heads) == 0 {
		return nil
	}
	return t.heads[0]
}

// Shift replaces the current best head with the next one from the same account.
func (t *TransactionsByPriceAndNonce) Shift() {
	acc, _ := Sender(t.signer, t.heads[0])
	if txs, ok := t.txs[acc]; ok && len(txs) > 0 {
		t.heads[0], t.txs[acc] = txs[0], txs[1:]
		heap.Fix(&t.heads, 0)
	} else {
		heap.Pop(&t.heads)
	}
}

// Pop removes the best transaction, *not* replacing it with the next one from
// the same account. This should be used when a transaction cannot be executed
// and hence all subsequent ones should be discarded from the same account.
func (t *TransactionsByPriceAndNonce) Pop() {
	heap.Pop(&t.heads)
}

// CKeys implement keys involved in a cross-shard transaction
type CKeys struct {
	Addr  common.Address
	Keys  []common.Hash
	WKeys []common.Hash
}

// AddKey adds a key to the CKey
func (ck *CKeys) AddKey(key common.Hash) {
	ck.Keys = append(ck.Keys, key)
}

// KeyVal stores both address and data
type KeyVal struct {
	Addr    common.Address
	Balance uint64
	Nonce   uint64
	Keys    []common.Hash
	Data    []common.Hash
}

// MKeyVal stores the data in a map format!
type MKeyVal struct {
	Addr    common.Address
	Balance uint64
	Nonce   uint64
	Data    map[common.Hash]common.Hash // key::value
}

// NewMKeyVal creates an new value!
func NewMKeyVal(addr common.Address, bal uint64, nonce uint64) *MKeyVal {
	return &MKeyVal{
		Addr:    addr,
		Balance: bal,
		Nonce:   nonce,
		Data:    make(map[common.Hash]common.Hash),
	}
}

type CData struct {
	Addr    common.Address
	Balance uint64
	Nonce   uint64
	Data    map[common.Hash]common.Hash
}

// TCommit of a cross-shard transaction
type TCommit struct {
	TxHash    common.Hash
	Shard     uint64
	BNum      uint64
	StateRoot common.Hash
}

// TCommits to store commitments of a cross-shard transaction
type TCommits struct {
	Lock    sync.RWMutex
	Commits map[uint64]*TCommit // shard:commit
}

// NewTCommits create a new commitments
func NewTCommits() *TCommits {
	return &TCommits{
		Commits: make(map[uint64]*TCommit),
	}
}

// AddCommit adds a commit for some particular shard
func (tcs *TCommits) AddCommit(shard uint64, commit *TCommit) int {
	tcs.Lock.Lock()
	defer tcs.Lock.Unlock()
	if _, ok := tcs.Commits[shard]; !ok {
		tcs.Commits[shard] = commit
	}
	return len(tcs.Commits)
}

// GetCommit returns commitment of a shard
func (tcs *TCommits) GetCommit(shard uint64) *TCommit {
	tcs.Lock.RLock()
	defer tcs.Lock.RUnlock()
	return tcs.Commits[shard]
}

// CrossTx is a object from cross-shard  Transaction
type CrossTx struct {
	Shards       []uint64
	BlockNum     *big.Int
	Tx           *Transaction
	AllContracts map[uint64][]*CKeys
}

func (ctx *CrossTx) SetTransaction(tx *Transaction) {
	ctx.Tx = &Transaction{data: tx.TxData()}
}

// GetRWSet to get read-write set per shard of a cross-shard trasnaction
func GetRWSet(numContracts uint16, index uint16, data []byte) ([]*CKeys, uint16) {
	var (
		allKeys []*CKeys
		u20     = uint16(20)
		u32     = uint16(32)
		addr    common.Address
		numKeys uint16
	)
	for i := uint16(0); i < numContracts; i++ {
		// Extracting keys per contarct
		addr = common.BytesToAddress(data[index : index+u20])
		index += u20
		numKeys = binary.BigEndian.Uint16(data[index : index+2])
		index += 2
		cKeys := &CKeys{Addr: addr, Keys: []common.Hash{}}
		for k := uint16(0); k < numKeys; k++ {
			key := common.BytesToHash(data[index : index+u32])
			index += u32
			cKeys.Keys = append(cKeys.Keys, key)
			// Checking whether the key is written to or not, if so
			// add to writekeys
			isWrite := int(data[index]) == 1
			if isWrite {
				cKeys.WKeys = append(cKeys.WKeys, key)
			}
			index++
		}
		// Adding all keys to a list
		allKeys = append(allKeys, cKeys)
	}
	return allKeys, index
}

// GetIntraRWSet extracts the read-write set of a intra-shard transactions
// Maintaining the format so that we can use existing code.
func GetIntraRWSet(shard uint64, data []byte) map[uint64][]*CKeys {
	var (
		sKeys   []*CKeys
		u20     = uint16(20)
		u32     = uint16(32)
		addr    common.Address
		allKeys = make(map[uint64][]*CKeys)
		index   = uint16(0)
	)
	numContracts := binary.BigEndian.Uint16(data[index:2])
	index += 2
	for i := uint16(0); i < numContracts; i++ {
		addr = common.BytesToAddress(data[index : index+u20])
		index += u20
		numKeys := binary.BigEndian.Uint16(data[index : index+2])
		index += 2
		cKeys := &CKeys{Addr: addr, Keys: []common.Hash{}}
		for k := uint16(0); k < numKeys; k++ {
			key := common.BytesToHash(data[index : index+u32])
			index += u32
			cKeys.Keys = append(cKeys.Keys, key)

			// Checking if the key is been written to or not
			if int(data[index]) == 1 {
				cKeys.WKeys = append(cKeys.WKeys, key)
			}
			index++
		}
		sKeys = append(sKeys, cKeys)
	}
	allKeys[shard] = sKeys
	return allKeys
}

// GetAllRWSet return all read-write set used in a cross-shard transaction
func GetAllRWSet(numShard uint16, data []byte) (map[uint64][]*CKeys, []uint64, uint16) {
	var (
		index        = uint16(0)
		numContracts uint16
		allKeys      []*CKeys // list of addr:keys for a given shard
		shards       []uint64
		allContracts = make(map[uint64][]*CKeys) // map shard: {list of addr:keys}
	)

	for i := uint16(0); i < numShard; i++ {
		shard := binary.BigEndian.Uint16(data[index : index+2])
		shards = append(shards, uint64(shard))
		index += 2
		numContracts = binary.BigEndian.Uint16(data[index : index+2])
		index += 2
		if numContracts > 0 {
			allKeys, index = GetRWSet(numContracts, index, data)
		}
		allContracts[uint64(shard)] = allKeys
	}
	return allContracts, shards, index
}

func ParseCrossTxData(numShard uint16, data []byte) *CrossTx {
	var (
		index    uint16
		addrSize = uint16(20)
		u8       = uint16(8)
		u32      = uint16(32)
	)
	ctx := &CrossTx{
		Shards:       []uint64{},
		AllContracts: make(map[uint64][]*CKeys),
	}
	ctx.AllContracts, ctx.Shards, index = GetAllRWSet(numShard, data)

	sender := common.BytesToAddress(data[index : index+addrSize])
	index += addrSize
	nonce := binary.BigEndian.Uint64(data[index : index+u8])
	index += u8
	value := new(big.Int)
	value.SetBytes(data[index : index+u32])
	index += u32
	receiver := common.BytesToAddress(data[index : index+addrSize])
	index += addrSize
	gasLimit := binary.BigEndian.Uint64(data[index : index+u8])
	index += u8
	gasPrice := binary.BigEndian.Uint64(data[index : index+u8])
	index += u8

	tx := NewCrossTransaction(CrossShardLocal, uint64(nonce), uint64(0), receiver, sender, value, gasLimit, big.NewInt(int64(gasPrice)), data[index:])
	ctx.SetTransaction(tx)

	log.Debug("New Cross shard Transaction", "hash", ctx.Tx.Hash(), "from", ctx.Tx.From(), "to", ctx.Tx.To(), "nonce", ctx.Tx.Nonce(), "value", ctx.Tx.Value(), "params", hex.EncodeToString(data[index:]))
	return ctx
}

// DecodeCrossTx extracts shards
func DecodeCrossTx(myshard uint64, data []byte) (uint64, []uint64, bool) {
	elemSize := uint64(32)
	lenData := data[2*elemSize+elemSize-8 : 3*elemSize]
	length := binary.BigEndian.Uint64(lenData)
	index := 3 * elemSize
	var (
		involved = false
		shards   []uint64
		u24      = uint64(24)
		u32      = uint64(32)
	)
	for i := uint64(0); i < length; i++ {
		shardData := data[index+u24 : index+u32]
		index += u32
		shard := binary.BigEndian.Uint64(shardData)
		if shard == myshard {
			involved = true
		}
		shards = append(shards, shard)
	}
	return index, shards, involved
}

// DecodeAck decodes an acknowledgement
func DecodeAck(stx *Transaction) (uint64, uint64, uint64, common.Hash) {
	var (
		u32   = 32
		u24   = 24
		index = 0
	)
	data := stx.Data()[4:]
	shard := binary.BigEndian.Uint64(data[index+u24 : index+u32])
	index += u32
	tid := binary.BigEndian.Uint64(data[index+u24 : index+u32])
	index += u32
	block := binary.BigEndian.Uint64(data[index+u24 : index+u32])
	index += u32
	tHash := common.BytesToHash(data[index:])
	return shard, tid, block, tHash
}

// DecodeDecision decodes elements of a local decision
func DecodeDecision(local bool, tx *Transaction) (uint64, uint64, uint64, common.Hash, common.Hash) {
	var (
		u32   = 32
		u24   = 24
		index = 0
	)
	data := tx.Data()[4:]
	shard := binary.BigEndian.Uint64(data[index+u24 : index+u32])
	index += u32
	txID := binary.BigEndian.Uint64(data[index+u24 : index+u32])
	index += u32
	bNum := binary.BigEndian.Uint64(data[index+u24 : index+u32])
	index += u32
	tHash := common.BytesToHash(data[index : index+u32])
	root := common.Hash{}
	if !local {
		index += u32
		root = common.BytesToHash(data[index : index+u32])
	}
	return shard, txID, bNum, tHash, root
}

// RWLock stores read write locks
type RWLock struct {
	Mu    sync.RWMutex
	Locks map[common.Address]*CLock
}

// NewRWLock creates new instance of RWLock
func NewRWLock() *RWLock {
	return &RWLock{
		Mu:    sync.RWMutex{},
		Locks: make(map[common.Address]*CLock),
	}
}

// ResetLock cleans existing values
func (rwl *RWLock) ResetLock() {
	rwl.Mu.Lock()
	defer rwl.Mu.Unlock()
	rwl.Locks = make(map[common.Address]*CLock)
}

// CLock stores currently locked keys of a contract
type CLock struct {
	Addr    common.Address
	ClockMu sync.RWMutex
	Keys    map[common.Hash]int
}

// NewCLock returns a new lock object
func NewCLock(addr common.Address) *CLock {
	return &CLock{
		Addr: addr,
		Keys: make(map[common.Hash]int),
	}
}

// TxControl stores foreign data for one block
type TxControl struct {
	TxControlMu  sync.RWMutex
	Tx           *Transaction
	TxID         uint64
	RefNum       uint64
	Status       bool
	Required     int
	Received     int                       // overall data avaiability status
	Keyval       map[common.Address]*CKeys // list of (k,v) pairs for each contract
	AddrToShard  map[common.Address]uint64 // addr to shard mapping
	ShardStatus  map[uint64]bool           // shard to its status mapping
	CommitStatus map[uint64]bool           // shard to its commit status mapping
	Commits      *TCommits                 // Corresponding commit
	Values       map[common.Address]*CData // key-value pair per contract
}

// NewTxControl creates a new datacache
func NewTxControl(rNum uint64, status bool) *TxControl {
	return &TxControl{
		RefNum:      rNum,
		Status:      status,
		TxID:        0,
		Required:    0,
		Received:    0,
		Keyval:      make(map[common.Address]*CKeys),
		AddrToShard: make(map[common.Address]uint64),
		ShardStatus: make(map[uint64]bool),
		Commits:     NewTCommits(),
		Values:      make(map[common.Address]*CData),
	}
}

// AddData adds data corresponding to keys
func (tcb *TxControl) AddData(shard uint64, mkvals map[common.Address]*MKeyVal) bool {
	tcb.TxControlMu.Lock()
	defer tcb.TxControlMu.Unlock()
	// If no address from that particular shard, return current status!
	if _, sok := tcb.ShardStatus[shard]; !sok {
		return tcb.Status
	}
	if !tcb.ShardStatus[shard] && len(mkvals) > 0 {
		for addr, keys := range tcb.Keyval {
			if tcb.AddrToShard[addr] != shard {
				continue
			}
			mkv := mkvals[addr]
			cdata := &CData{
				Addr:    addr,
				Nonce:   mkv.Nonce,
				Balance: mkv.Balance,
				Data:    make(map[common.Hash]common.Hash),
			}
			for _, key := range keys.Keys {
				cdata.Data[key] = mkv.Data[key]
			}
			tcb.Values[addr] = cdata
			log.Debug("Adding data for", "addr", cdata.Addr, "nonce", cdata.Nonce)
		}
		tcb.ShardStatus[shard] = true
		tcb.Received++
		if tcb.Received == tcb.Required {
			tcb.Status = true
		}
	}
	return tcb.Status
}

// InitTxControl adds transaction detail
func (tcb *TxControl) InitTxControl(myshard, txID uint64, ctx *CrossTx) {
	tcb.TxControlMu.Lock()
	defer tcb.TxControlMu.Unlock()
	tcb.Received = 0
	tcb.Required = 0
	tcb.Tx = ctx.Tx
	tcb.TxID = txID

	for shard, allKeys := range ctx.AllContracts {
		if _, ok := tcb.ShardStatus[shard]; !ok {
			if shard != myshard {
				tcb.Required++
				tcb.ShardStatus[shard] = false
			} else {
				tcb.ShardStatus[shard] = true
			}
		}
		for _, contract := range allKeys {
			caddr := contract.Addr
			if _, cok := tcb.AddrToShard[caddr]; !cok {
				tcb.AddrToShard[caddr] = shard
				tcb.Keyval[caddr] = &CKeys{Addr: caddr}
			}
			for _, key := range contract.Keys {
				tcb.Keyval[caddr].AddKey(key)
			}
			for _, key := range contract.WKeys {
				tcb.Keyval[caddr].AddKey(key)
			}
			log.Debug("Adding keys", "addr", caddr, "shard", shard)
		}
	}
}

// AddTCommit adds commitment for a transaction
func (tcb *TxControl) AddTCommit(commit *TCommit) bool {
	tcb.TxControlMu.Lock()
	defer tcb.TxControlMu.Unlock()

	count := tcb.Commits.AddCommit(commit.Shard, commit)
	if count == tcb.Required {
		return true
	}
	return false
}

// UpdateLocalStatus Update data status for local shard
func (tcb *TxControl) UpdateLocalStatus(myshard uint64) {
	tcb.TxControlMu.Lock()
	defer tcb.TxControlMu.Unlock()
	if !tcb.ShardStatus[myshard] {
		tcb.ShardStatus[myshard] = true
		tcb.Received++
	}
}

// Message is a fully derived transaction and implements core.Message
//
// NOTE: In a future PR this will be removed.
type Message struct {
	to         *common.Address
	from       common.Address
	txType     uint64
	nonce      uint64
	shard      uint64
	amount     *big.Int
	gasLimit   uint64
	gasPrice   *big.Int
	data       []byte
	checkNonce bool
	isPrivate  bool
}

func NewMessage(from common.Address, to *common.Address, nonce, txType, shard uint64, amount *big.Int, gasLimit uint64, gasPrice *big.Int, data []byte, checkNonce bool) Message {
	return Message{
		from:       from,
		to:         to,
		nonce:      nonce,
		txType:     txType,
		shard:      shard,
		amount:     amount,
		gasLimit:   gasLimit,
		gasPrice:   gasPrice,
		data:       data,
		checkNonce: checkNonce,
	}
}

func (m Message) From() common.Address { return m.from }
func (m Message) To() *common.Address  { return m.to }
func (m Message) GasPrice() *big.Int   { return m.gasPrice }
func (m Message) Value() *big.Int      { return m.amount }
func (m Message) Gas() uint64          { return m.gasLimit }
func (m Message) Nonce() uint64        { return m.nonce }
func (m Message) TxType() uint64       { return m.txType }
func (m Message) Shard() uint64        { return m.shard }
func (m Message) Data() []byte         { return m.data }
func (m Message) CheckNonce() bool     { return m.checkNonce }

func (m Message) IsPrivate() bool {
	return m.isPrivate
}

func (tx *Transaction) IsPrivate() bool {
	if tx.data.V == nil {
		return false
	}
	return tx.data.V.Uint64() == 37 || tx.data.V.Uint64() == 38
}

/*
 * Indicates that a transaction is private, but doesn't necessarily set the correct v value, as it can be called on
 * an unsigned transaction.
 * pre homestead signer, all v values were v=27 or v=28, with EIP155Signer that change,
 * but SetPrivate() is also used on unsigned transactions to temporarily set the v value to indicate
 * the transaction is intended to be private, and so that the correct signer can be selected. The signer will correctly
 * set the valid v value (37 or 38): This helps minimize changes vs upstream go-ethereum code.
 */
func (tx *Transaction) SetPrivate() {
	if tx.IsPrivate() {
		return
	}
	if tx.data.V.Int64() == 28 {
		tx.data.V.SetUint64(38)
	} else {
		tx.data.V.SetUint64(37)
	}
}
