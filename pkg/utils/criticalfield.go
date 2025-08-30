package utils

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"

	"github.com/cbergoon/merkletree"
)

// An object of type CriticalField implements the merkletree.Content interface.
type CriticalField struct {
	FieldPath []string
	Value     string
}

func (cf *CriticalField) CalculateHash() ([]byte, error) {
	fieldJSON, err := json.Marshal(cf.FieldPath)
	if err != nil {
		return nil, err
	}
	fieldHash := sha256.Sum256(fieldJSON)
	valueHash := sha256.Sum256([]byte(cf.Value))
	contentHash := sha256.Sum256(append(fieldHash[:], valueHash[:]...))
	return contentHash[:], nil
}

func (cf *CriticalField) Equals(other merkletree.Content) (bool, error) {
	lhsHash, err := cf.CalculateHash()
	if err != nil {
		return false, err
	}

	rhsHash, err := other.CalculateHash()
	if err != nil {
		return false, err
	}

	return bytes.Equal(lhsHash, rhsHash), nil
}
