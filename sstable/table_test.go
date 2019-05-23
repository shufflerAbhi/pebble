// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/kr/pretty"
	"github.com/petermattis/pebble/bloom"
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/vfs"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

// nonsenseWords are words that aren't in testdata/h.txt.
var nonsenseWords = []string{
	// Edge cases.
	"",
	"\x00",
	"\xff",
	"`",
	"a\x00",
	"aaaaaa",
	"pol\x00nius",
	"youth\x00",
	"youti",
	"zzzzzz",
	// Capitalized versions of actual words in testdata/h.txt.
	"A",
	"Hamlet",
	"thEE",
	"YOUTH",
	// The following were generated by http://soybomb.com/tricks/words/
	"pectures",
	"exectly",
	"tricatrippian",
	"recens",
	"whiratroce",
	"troped",
	"balmous",
	"droppewry",
	"toilizing",
	"crocias",
	"eathrass",
	"cheakden",
	"speablett",
	"skirinies",
	"prefing",
	"bonufacision",
}

var (
	wordCount = map[string]string{}
	minWord   = ""
	maxWord   = ""
)

func init() {
	f, err := os.Open(filepath.FromSlash("testdata/h.txt"))
	if err != nil {
		panic(err)
	}
	defer f.Close()
	r := bufio.NewReader(f)

	for first := true; ; {
		s, err := r.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		k := strings.TrimSpace(string(s[8:]))
		v := strings.TrimSpace(string(s[:8]))
		wordCount[k] = v

		if first {
			first = false
			minWord = k
			maxWord = k
			continue
		}
		if minWord > k {
			minWord = k
		}
		if maxWord < k {
			maxWord = k
		}
	}

	if len(wordCount) != 1710 {
		panic(fmt.Sprintf("h.txt entry count: got %d, want %d", len(wordCount), 1710))
	}

	for _, s := range nonsenseWords {
		if _, ok := wordCount[s]; ok {
			panic(fmt.Sprintf("nonsense word %q was in h.txt", s))
		}
	}
}

func check(f vfs.File, comparer *db.Comparer, fp db.FilterPolicy) error {
	r := NewReader(f, 0, &db.Options{
		Comparer: comparer,
		Levels: []db.LevelOptions{{
			FilterPolicy: fp,
		}},
	})

	// Check that each key/value pair in wordCount is also in the table.
	words := make([]string, 0, len(wordCount))
	for k, v := range wordCount {
		words = append(words, k)
		// Check using Get.
		if v1, err := r.get([]byte(k)); string(v1) != string(v) || err != nil {
			return fmt.Errorf("Get %q: got (%q, %v), want (%q, %v)", k, v1, err, v, error(nil))
		} else if len(v1) != cap(v1) {
			return fmt.Errorf("Get %q: len(v1)=%d, cap(v1)=%d", k, len(v1), cap(v1))
		}

		// Check using SeekGE.
		i := iterAdapter{r.NewIter(nil /* lower */, nil /* upper */)}
		if !i.SeekGE([]byte(k)) || string(i.Key().UserKey) != k {
			return fmt.Errorf("Find %q: key was not in the table", k)
		}
		if k1 := i.Key().UserKey; len(k1) != cap(k1) {
			return fmt.Errorf("Find %q: len(k1)=%d, cap(k1)=%d", k, len(k1), cap(k1))
		}
		if string(i.Value()) != v {
			return fmt.Errorf("Find %q: got value %q, want %q", k, i.Value(), v)
		}
		if v1 := i.Value(); len(v1) != cap(v1) {
			return fmt.Errorf("Find %q: len(v1)=%d, cap(v1)=%d", k, len(v1), cap(v1))
		}

		// Check using SeekLT.
		if !i.SeekLT([]byte(k)) {
			i.First()
		} else {
			i.Next()
		}
		if string(i.Key().UserKey) != k {
			return fmt.Errorf("Find %q: key was not in the table", k)
		}
		if k1 := i.Key().UserKey; len(k1) != cap(k1) {
			return fmt.Errorf("Find %q: len(k1)=%d, cap(k1)=%d", k, len(k1), cap(k1))
		}
		if string(i.Value()) != v {
			return fmt.Errorf("Find %q: got value %q, want %q", k, i.Value(), v)
		}
		if v1 := i.Value(); len(v1) != cap(v1) {
			return fmt.Errorf("Find %q: len(v1)=%d, cap(v1)=%d", k, len(v1), cap(v1))
		}

		if err := i.Close(); err != nil {
			return err
		}
	}

	// Check that nonsense words are not in the table.
	for _, s := range nonsenseWords {
		// Check using Get.
		if _, err := r.get([]byte(s)); err != db.ErrNotFound {
			return fmt.Errorf("Get %q: got %v, want ErrNotFound", s, err)
		}

		// Check using Find.
		i := iterAdapter{r.NewIter(nil /* lower */, nil /* upper */)}
		if i.SeekGE([]byte(s)) && s == string(i.Key().UserKey) {
			return fmt.Errorf("Find %q: unexpectedly found key in the table", s)
		}
		if err := i.Close(); err != nil {
			return err
		}
	}

	// Check that the number of keys >= a given start key matches the expected number.
	var countTests = []struct {
		count int
		start string
	}{
		// cat h.txt | cut -c 9- | wc -l gives 1710.
		{1710, ""},
		// cat h.txt | cut -c 9- | grep -v "^[a-b]" | wc -l gives 1522.
		{1522, "c"},
		// cat h.txt | cut -c 9- | grep -v "^[a-j]" | wc -l gives 940.
		{940, "k"},
		// cat h.txt | cut -c 9- | grep -v "^[a-x]" | wc -l gives 12.
		{12, "y"},
		// cat h.txt | cut -c 9- | grep -v "^[a-z]" | wc -l gives 0.
		{0, "~"},
	}
	for _, ct := range countTests {
		n, i := 0, iterAdapter{r.NewIter(nil /* lower */, nil /* upper */)}
		for valid := i.SeekGE([]byte(ct.start)); valid; valid = i.Next() {
			n++
		}
		if n != ct.count {
			return fmt.Errorf("count %q: got %d, want %d", ct.start, n, ct.count)
		}
		n = 0
		for valid := i.Last(); valid; valid = i.Prev() {
			if bytes.Compare(i.Key().UserKey, []byte(ct.start)) < 0 {
				break
			}
			n++
		}
		if n != ct.count {
			return fmt.Errorf("count %q: got %d, want %d", ct.start, n, ct.count)
		}
		if err := i.Close(); err != nil {
			return err
		}
	}

	// Check lower/upper bounds behavior. Randomly choose a lower and upper bound
	// and then guarantee that iteration finds the expected number if entries.
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	sort.Strings(words)
	for i := 0; i < 10; i++ {
		lowerIdx := -1
		upperIdx := len(words)
		if rng.Intn(5) != 0 {
			lowerIdx = rng.Intn(len(words))
		}
		if rng.Intn(5) != 0 {
			upperIdx = rng.Intn(len(words))
		}
		if lowerIdx > upperIdx {
			lowerIdx, upperIdx = upperIdx, lowerIdx
		}

		var lower, upper []byte
		if lowerIdx >= 0 {
			lower = []byte(words[lowerIdx])
		} else {
			lowerIdx = 0
		}
		if upperIdx < len(words) {
			upper = []byte(words[upperIdx])
		}

		i := iterAdapter{r.NewIter(lower, upper)}

		{
			// NB: the semantics of First are that it starts iteration from the
			// beginning, not respecting the lower bound.
			n := 0
			for valid := i.First(); valid; valid = i.Next() {
				n++
			}
			if expected := upperIdx; expected != n {
				return fmt.Errorf("expected %d, but found %d", expected, n)
			}
		}

		{
			// NB: the semantics of Last are that it starts iteration from the end, not
			// respecting the upper bound.
			n := 0
			for valid := i.Last(); valid; valid = i.Prev() {
				n++
			}
			if expected := len(words) - lowerIdx; expected != n {
				return fmt.Errorf("expected %d, but found %d", expected, n)
			}
		}

		if lower != nil {
			n := 0
			for valid := i.SeekGE(lower); valid; valid = i.Next() {
				n++
			}
			if expected := upperIdx - lowerIdx; expected != n {
				return fmt.Errorf("expected %d, but found %d", expected, n)
			}
		}

		if upper != nil {
			n := 0
			for valid := i.SeekLT(upper); valid; valid = i.Prev() {
				n++
			}
			if expected := upperIdx - lowerIdx; expected != n {
				return fmt.Errorf("expected %d, but found %d", expected, n)
			}
		}

		if err := i.Close(); err != nil {
			return err
		}
	}

	return r.Close()
}

var (
	memFileSystem = vfs.NewMem()
	tmpFileCount  int
)

func build(
	compression db.Compression,
	fp db.FilterPolicy,
	ftype db.FilterType,
	comparer *db.Comparer,
	propCollector func() db.TablePropertyCollector,
) (vfs.File, error) {
	// Create a sorted list of wordCount's keys.
	keys := make([]string, len(wordCount))
	i := 0
	for k := range wordCount {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	// Write the key/value pairs to a new table, in increasing key order.
	filename := fmt.Sprintf("/tmp%d", tmpFileCount)
	f0, err := memFileSystem.Create(filename)
	if err != nil {
		return nil, err
	}
	defer f0.Close()
	tmpFileCount++

	opts := &db.Options{
		Merger: &db.Merger{
			Name: "nullptr",
		},
		Comparer: comparer,
	}
	if propCollector != nil {
		opts.TablePropertyCollectors = append(opts.TablePropertyCollectors, propCollector)
	}

	levelOpts := db.LevelOptions{
		Compression:  compression,
		FilterPolicy: fp,
		FilterType:   ftype,
	}

	w := NewWriter(f0, opts, levelOpts)
	for _, k := range keys {
		v := wordCount[k]
		ikey := db.MakeInternalKey([]byte(k), 0, db.InternalKeyKindSet)
		if err := w.Add(ikey, []byte(v)); err != nil {
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	// Re-open that filename for reading.
	f1, err := memFileSystem.Open(filename)
	if err != nil {
		return nil, err
	}
	return f1, nil
}

func testReader(t *testing.T, filename string, comparer *db.Comparer, fp db.FilterPolicy) {
	// Check that we can read a pre-made table.
	f, err := os.Open(filepath.FromSlash("testdata/" + filename))
	if err != nil {
		t.Error(err)
		return
	}
	err = check(f, comparer, fp)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestReaderLevelDB(t *testing.T)            { testReader(t, "h.ldb", nil, nil) }
func TestReaderDefaultCompression(t *testing.T) { testReader(t, "h.sst", nil, nil) }
func TestReaderNoCompression(t *testing.T)      { testReader(t, "h.no-compression.sst", nil, nil) }
func TestReaderBlockBloomIgnored(t *testing.T) {
	testReader(t, "h.block-bloom.no-compression.sst", nil, nil)
}
func TestReaderTableBloomIgnored(t *testing.T) {
	testReader(t, "h.table-bloom.no-compression.sst", nil, nil)
}

func TestReaderBloomUsed(t *testing.T) {
	// wantActualNegatives is the minimum number of nonsense words (i.e. false
	// positives or true negatives) to run through our filter. Some nonsense
	// words might be rejected even before the filtering step, if they are out
	// of the [minWord, maxWord] range of keys in the table.
	wantActualNegatives := 0
	for _, s := range nonsenseWords {
		if minWord < s && s < maxWord {
			wantActualNegatives++
		}
	}

	files := []struct {
		path     string
		comparer *db.Comparer
	}{
		{"h.table-bloom.no-compression.sst", nil},
		{"h.table-bloom.no-compression.prefix_extractor.no_whole_key_filter.sst", fixtureComparer},
	}
	for _, tc := range files {
		t.Run(tc.path, func(t *testing.T) {
			for _, degenerate := range []bool{false, true} {
				t.Run(fmt.Sprintf("degenerate=%t", degenerate), func(t *testing.T) {
					c := &countingFilterPolicy{
						FilterPolicy: bloom.FilterPolicy(10),
						degenerate:   degenerate,
					}
					testReader(t, tc.path, tc.comparer, c)

					if c.truePositives != len(wordCount) {
						t.Errorf("degenerate=%t: true positives: got %d, want %d", degenerate, c.truePositives, len(wordCount))
					}
					if c.falseNegatives != 0 {
						t.Errorf("degenerate=%t: false negatives: got %d, want %d", degenerate, c.falseNegatives, 0)
					}

					if got := c.falsePositives + c.trueNegatives; got < wantActualNegatives {
						t.Errorf("degenerate=%t: actual negatives (false positives + true negatives): "+
							"got %d (%d + %d), want >= %d",
							degenerate, got, c.falsePositives, c.trueNegatives, wantActualNegatives)
					}

					if !degenerate {
						// The true negative count should be much greater than the false
						// positive count.
						if c.trueNegatives < 10*c.falsePositives {
							t.Errorf("degenerate=%t: true negative to false positive ratio (%d:%d) is too small",
								degenerate, c.trueNegatives, c.falsePositives)
						}
					}
				})
			}
		})
	}
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	f, err := os.Open(filepath.FromSlash("testdata/h.table-bloom.no-compression.sst"))
	if err != nil {
		t.Fatal(err)
	}
	c := &countingFilterPolicy{
		FilterPolicy: bloom.FilterPolicy(1),
	}
	r := NewReader(f, 0, &db.Options{
		Levels: []db.LevelOptions{{
			FilterPolicy: c,
		}},
	})

	const n = 10000
	// key is a buffer that will be re-used for n Get calls, each with a
	// different key. The "m" in the 2-byte prefix means that the key falls in
	// the [minWord, maxWord] range and so will not be rejected prior to
	// applying the Bloom filter. The "!" in the 2-byte prefix means that the
	// key is not actually in the table. The filter will only see actual
	// negatives: false positives or true negatives.
	key := []byte("m!....")
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint32(key[2:6], uint32(i))
		r.get(key)
	}

	if c.truePositives != 0 {
		t.Errorf("true positives: got %d, want 0", c.truePositives)
	}
	if c.falseNegatives != 0 {
		t.Errorf("false negatives: got %d, want 0", c.falseNegatives)
	}
	if got := c.falsePositives + c.trueNegatives; got != n {
		t.Errorf("actual negatives (false positives + true negatives): got %d (%d + %d), want %d",
			got, c.falsePositives, c.trueNegatives, n)
	}

	// According the the comments in the C++ LevelDB code, the false positive
	// rate should be approximately 1% for for bloom.FilterPolicy(10). The 10
	// was the parameter used to write the .sst file. When reading the file,
	// the 1 in the bloom.FilterPolicy(1) above doesn't matter, only the
	// bloom.FilterPolicy matters.
	if got := float64(100*c.falsePositives) / n; got < 0.2 || 5 < got {
		t.Errorf("false positive rate: got %v%%, want approximately 1%%", got)
	}
}

type countingFilterPolicy struct {
	db.FilterPolicy
	degenerate bool

	truePositives  int
	falsePositives int
	falseNegatives int
	trueNegatives  int
}

func (c *countingFilterPolicy) MayContain(ftype db.FilterType, filter, key []byte) bool {
	got := true
	if c.degenerate {
		// When degenerate is true, we override the embedded FilterPolicy's
		// MayContain method to always return true. Doing so is a valid, if
		// inefficient, implementation of the FilterPolicy interface.
	} else {
		got = c.FilterPolicy.MayContain(ftype, filter, key)
	}
	_, want := wordCount[string(key)]

	switch {
	case got && want:
		c.truePositives++
	case got && !want:
		c.falsePositives++
	case !got && want:
		c.falseNegatives++
	case !got && !want:
		c.trueNegatives++
	}
	return got
}

func TestWriterRoundTrip(t *testing.T) {
	for name, fp := range map[string]db.FilterPolicy{
		"none":       nil,
		"bloom10bit": bloom.FilterPolicy(10),
	} {
		t.Run(fmt.Sprintf("bloom=%s", name), func(t *testing.T) {
			f, err := build(db.DefaultCompression, fp, db.TableFilter, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			// Check that we can read a freshly made table.

			err = check(f, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFinalBlockIsWritten(t *testing.T) {
	const blockSize = 100
	keys := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	valueLengths := []int{0, 1, 22, 28, 33, 40, 50, 61, 87, 100, 143, 200}
	xxx := bytes.Repeat([]byte("x"), valueLengths[len(valueLengths)-1])

	for nk := 0; nk <= len(keys); nk++ {
	loop:
		for _, vLen := range valueLengths {
			got, memFS := 0, vfs.NewMem()

			wf, err := memFS.Create("foo")
			if err != nil {
				t.Errorf("nk=%d, vLen=%d: memFS create: %v", nk, vLen, err)
				continue
			}
			w := NewWriter(wf, nil, db.LevelOptions{
				BlockSize: blockSize,
			})
			for _, k := range keys[:nk] {
				if err := w.Add(db.InternalKey{UserKey: []byte(k)}, xxx[:vLen]); err != nil {
					t.Errorf("nk=%d, vLen=%d: set: %v", nk, vLen, err)
					continue loop
				}
			}
			if err := w.Close(); err != nil {
				t.Errorf("nk=%d, vLen=%d: writer close: %v", nk, vLen, err)
				continue
			}

			rf, err := memFS.Open("foo")
			if err != nil {
				t.Errorf("nk=%d, vLen=%d: memFS open: %v", nk, vLen, err)
				continue
			}
			r := NewReader(rf, 0, nil)
			i := iterAdapter{r.NewIter(nil /* lower */, nil /* upper */)}
			for valid := i.First(); valid; valid = i.Next() {
				got++
			}
			if err := i.Close(); err != nil {
				t.Errorf("nk=%d, vLen=%d: Iterator close: %v", nk, vLen, err)
				continue
			}
			if err := r.Close(); err != nil {
				t.Errorf("nk=%d, vLen=%d: reader close: %v", nk, vLen, err)
				continue
			}

			if got != nk {
				t.Errorf("nk=%2d, vLen=%3d: got %2d keys, want %2d", nk, vLen, got, nk)
				continue
			}
		}
	}
}

func TestReaderGlobalSeqNum(t *testing.T) {
	f, err := os.Open(filepath.FromSlash("testdata/h.sst"))
	if err != nil {
		t.Fatal(err)
	}
	r := NewReader(f, 0, nil)
	defer r.Close()

	const globalSeqNum = 42
	r.Properties.GlobalSeqNum = globalSeqNum

	i := iterAdapter{r.NewIter(nil /* lower */, nil /* upper */)}
	for valid := i.First(); valid; valid = i.Next() {
		if globalSeqNum != i.Key().SeqNum() {
			t.Fatalf("expected %d, but found %d", globalSeqNum, i.Key().SeqNum())
		}
	}
}

func TestFooterRoundTrip(t *testing.T) {
	buf := make([]byte, 100+maxFooterLen)
	for _, format := range []db.TableFormat{
		db.TableFormatRocksDBv2,
		db.TableFormatLevelDB,
	} {
		t.Run(fmt.Sprintf("format=%d", format), func(t *testing.T) {
			for _, checksum := range []uint8{checksumCRC32c} {
				t.Run(fmt.Sprintf("checksum=%d", checksum), func(t *testing.T) {
					footer := footer{
						format:      format,
						checksum:    checksum,
						metaindexBH: blockHandle{offset: 1, length: 2},
						indexBH:     blockHandle{offset: 3, length: 4},
					}
					for offset := range []int64{0, 1, 100} {
						t.Run(fmt.Sprintf("offset=%d", offset), func(t *testing.T) {
							mem := vfs.NewMem()
							f, err := mem.Create("test")
							if err != nil {
								t.Fatal(err)
							}
							if _, err := f.Write(buf[:offset]); err != nil {
								t.Fatal(err)
							}
							if _, err := f.Write(footer.encode(buf[100:])); err != nil {
								t.Fatal(err)
							}
							if err := f.Close(); err != nil {
								t.Fatal(err)
							}

							f, err = mem.Open("test")
							if err != nil {
								t.Fatal(err)
							}
							result, err := readFooter(f)
							if err != nil {
								t.Fatal(err)
							}
							if err := f.Close(); err != nil {
								t.Fatal(err)
							}

							if diff := pretty.Diff(footer, result); diff != nil {
								t.Fatalf("expected %+v, but found %+v\n%s",
									footer, result, strings.Join(diff, "\n"))
							}
						})
					}
				})
			}
		})
	}
}

func TestReadFooter(t *testing.T) {
	encode := func(format db.TableFormat, checksum uint8) string {
		f := footer{
			format:   format,
			checksum: checksum,
		}
		return string(f.encode(make([]byte, maxFooterLen)))
	}

	testCases := []struct {
		encoded  string
		expected string
	}{
		{strings.Repeat("a", minFooterLen-1), "file size is too small"},
		{strings.Repeat("a", levelDBFooterLen), "bad magic number"},
		{strings.Repeat("a", rocksDBFooterLen), "bad magic number"},
		{encode(db.TableFormatLevelDB, 0)[1:], "file size is too small"},
		{encode(db.TableFormatRocksDBv2, 0)[1:], "footer too short"},
		{encode(db.TableFormatRocksDBv2, noChecksum), "unsupported checksum type"},
		{encode(db.TableFormatRocksDBv2, checksumXXHash), "unsupported checksum type"},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			mem := vfs.NewMem()
			f, err := mem.Create("test")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write([]byte(c.encoded)); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}

			f, err = mem.Open("test")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := readFooter(f); err == nil {
				t.Fatalf("expected %q, but found success", c.expected)
			} else if !strings.Contains(err.Error(), c.expected) {
				t.Fatalf("expected %q, but found %v", c.expected, err)
			}
		})
	}
}

type errorPropCollector struct{}

func (errorPropCollector) Add(key db.InternalKey, _ []byte) error {
	return fmt.Errorf("add %s failed", key)
}

func (errorPropCollector) Finish(_ map[string]string) error {
	return fmt.Errorf("finish failed")
}

func (errorPropCollector) Name() string {
	return "errorPropCollector"
}

func TestTablePropertyCollectorErrors(t *testing.T) {
	mem := vfs.NewMem()
	f, err := mem.Create("foo")
	if err != nil {
		t.Fatal(err)
	}

	opts := &db.Options{}
	opts.TablePropertyCollectors = append(opts.TablePropertyCollectors,
		func() db.TablePropertyCollector {
			return errorPropCollector{}
		})

	w := NewWriter(f, opts, db.LevelOptions{})
	require.Regexp(t, `add a#0,1 failed`, w.Set([]byte("a"), []byte("b")))
	require.Regexp(t, `add c#0,0 failed`, w.Delete([]byte("c")))
	require.Regexp(t, `add d#0,15 failed`, w.DeleteRange([]byte("d"), []byte("e")))
	require.Regexp(t, `add f#0,2 failed`, w.Merge([]byte("f"), []byte("g")))
	require.Regexp(t, `finish failed`, w.Close())
}
