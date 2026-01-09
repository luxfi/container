// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package iterator_test

import (
	"testing"
	"time"

	"github.com/google/btree"
	"github.com/stretchr/testify/require"

	"github.com/luxfi/ids"
	"github.com/luxfi/container/iterator"
)

var defaultTreeDegree = 2

type testStaker struct {
	TxID     ids.ID
	NextTime time.Time
}

func (s *testStaker) Less(other *testStaker) bool {
	return s.NextTime.Before(other.NextTime)
}

func TestTree(t *testing.T) {
	require := require.New(t)
	stakers := []*testStaker{
		{
			TxID:     ids.GenerateTestID(),
			NextTime: time.Unix(0, 0),
		},
		{
			TxID:     ids.GenerateTestID(),
			NextTime: time.Unix(1, 0),
		},
		{
			TxID:     ids.GenerateTestID(),
			NextTime: time.Unix(2, 0),
		},
	}

	tree := btree.NewG(defaultTreeDegree, (*testStaker).Less)
	for _, staker := range stakers {
		require.Nil(tree.ReplaceOrInsert(staker))
	}

	it := iterator.FromTree(tree)
	for _, staker := range stakers {
		require.True(it.Next())
		require.Equal(staker, it.Value())
	}
	require.False(it.Next())
	it.Release()
}

func TestTreeNil(t *testing.T) {
	it := iterator.FromTree[*testStaker](nil)
	require.False(t, it.Next())
	it.Release()
}

func TestTreeEarlyRelease(t *testing.T) {
	require := require.New(t)
	stakers := []*testStaker{
		{
			TxID:     ids.GenerateTestID(),
			NextTime: time.Unix(0, 0),
		},
		{
			TxID:     ids.GenerateTestID(),
			NextTime: time.Unix(1, 0),
		},
		{
			TxID:     ids.GenerateTestID(),
			NextTime: time.Unix(2, 0),
		},
	}

	tree := btree.NewG(defaultTreeDegree, (*testStaker).Less)
	for _, staker := range stakers {
		require.Nil(tree.ReplaceOrInsert(staker))
	}

	it := iterator.FromTree(tree)
	require.True(it.Next())
	it.Release()
	require.False(it.Next())
}
