package syncmap

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type mySuite struct {
	suite.Suite
}

func TestUnitTestSuite(t *testing.T) {
	suite.Run(t, &mySuite{})
}

func (s *mySuite) TestSyncMap_NonexistentString() {
	strMap := SyncMap[string, string]{}

	nadaStr, myOk := strMap.Load("foo")
	s.Assert().Empty(nadaStr, "Load() of string when key is nonexistent")
	s.Assert().False(myOk, "Load() indicates nonexistence")
}

func (s *mySuite) TestSyncMap_Range() {
	keys := []int32{}
	vals := []int64{}
	otherMap := SyncMap[int32, int64]{}
	otherMap.Store(1, 11)
	otherMap.Store(2, 22)
	otherMap.Store(3, 33)

	otherMap.Range(func(k int32, v int64) bool {
		keys = append(keys, k)
		vals = append(vals, v)
		return true
	})

	s.Assert().ElementsMatch(
		[]int32{1, 2, 3},
		keys,
		"Range(): keys as expected",
	)

	s.Assert().ElementsMatch(
		[]int64{11, 22, 33},
		vals,
		"Range(): values as expected",
	)
}

func (s *mySuite) TestSyncMap_Len() {
	theMap := SyncMap[string, []byte]{}

	theMap.Store("foo", []byte{})
	theMap.Store("bar", []byte{})
	theMap.Store("baz", []byte{})
	theMap.Store("qux", []byte{})

	s.Assert().Equal(4, theMap.Len(), "Len() gives the size")
}

func (s *mySuite) TestSyncMap_LoadOrStore() {
	theMap := SyncMap[string, []byte]{}

	theVal, ok := theMap.LoadOrStore("foo", []byte{2})
	s.Assert().Equal([]byte{2}, theVal, "LoadOrStore() returned new value if no old one exists")
	s.Assert().False(ok, "LoadOrStore() indicates nonexistence")

	theVal, ok = theMap.LoadOrStore("foo", []byte{3})
	s.Assert().Equal([]byte{2}, theVal, "LoadOrStore() returned old value")
	s.Assert().True(ok, "LoadOrStore() indicates existence")
}

func (s *mySuite) TestSyncMap_LoadAndDelete() {
	theMap := SyncMap[string, []byte]{}

	theVal, ok := theMap.LoadAndDelete("foo")
	s.Assert().Nil(theVal, "LoadAndDelete() returns nil on nonexistence")
	s.Assert().False(ok, "boolean indicates nonexistence")

	theMap.Store("foo", []byte{1})

	theVal, ok = theMap.LoadAndDelete("foo")
	s.Assert().Equal([]byte{1}, theVal, "LoadAndDelete() returned Store()d value")
	s.Assert().True(ok, "boolean indicates existence")
}

func (s *mySuite) TestSyncMap_Basics() {
	theMap := SyncMap[string, []byte]{}

	nada, ok := theMap.Load("foo")
	s.Assert().Nil(nada, "Load() of array when key is nonexistent")
	s.Assert().False(ok, "Load() indicates nonexistence")

	theMap.Store("foo", []byte{1})
	theVal, ok := theMap.Load("foo")
	s.Assert().Equal([]byte{1}, theVal, "Load() returned Store()d value")
	s.Assert().True(ok, "Load() indicates existence")

	theMap.Delete("foo")
	_, ok = theMap.Load("foo")
	s.Assert().False(ok, "Load() indicates nonexistence after Delete()")
}
