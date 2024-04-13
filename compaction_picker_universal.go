package pebble

import (
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// The main type definition for the Universal Compaction Picker.
// This type holds the state and implements the methods required
// for compaction picking
type compactionPickerUniversal struct {
	opts *Options
	vers *version

	// The level to target for L0 compactions. Levels L1 to baseLevel must be
	// empty. We use this only to set some properties in the picked compaction
	// object.
	baseLevel int

	// The files that are selected for periodic universal compaction
	filesMarkedForPeriodicCompaction []levelFileMetadata

	// The sorted runs used for compaction picking
	sortedRuns []sortedRunInfo
}

// A struct to hold a file along with its level in the
// LSM tree.
type levelFileMetadata struct {
	*manifest.FileMetadata
	level int
}

// A type that holds the information about a sorted run
// to be used for compaction picking. Each file in L0
// is a sorted run whereas for level > 0, the entire
// level makes up a single sorted run. So, to represent
// a sorted run in L0, we need to specify L0 and the
// specific file whereas for level > 0, just specifying
// the level is enough to specify the sorted run.
// TODO: This can be optimized since each L0 sublevel
// forms a single sorted run.
type sortedRunInfo struct {
	level int

	// `file` Will be nil for level > 0. For level = 0, the sorted run is
	// for this file.
	file *manifest.FileMetadata

	// For level > 0, `size` and `compensatedFileSize` are sum of sizes all
	// files in the level. For level = 0, these represent the size and
	// compensated size of a single file.
	size uint64

	// compensatedFileSize is a measure of the delete entries in the file.
	// The higher this value, the more beneficial it is if this file is
	// picked for compaction.
	compensatedFileSize uint64

	// `beingCompacted` represents if the sorted run is being compacted.
	// should be the same for all files in a non-zero level. That is,
	// if a single file in a sorted run is being compacted, we consider
	// the whole
	beingCompacted bool
}

// Pick a compaction based on Universal Compaction logic.
// This is the main entry point for Universal Compaction Picker and it returns the
// picked compaction.
func (p *compactionPickerUniversal) pickUniversalCompaction(env compactionEnv) (pc *pickedCompaction) {

	// There are 4 different sub cases for Universal Compaction. Currently
	// we've only implemented one - Periodic Compaction.

	// In RocksDB, whenever a new version is created, they run functions
	// which compute compaction scores as well as mark different files
	// that are eligible for different compaction modes. These values
	// are then used while picking the subsequent compaction. But
	// in Pebble, we just compute the values and mark the files
	// when we are about to pick a compaction.

	// Compute the sorted runs available for compaction
	p.computeSortedRuns(env)

	// Set up the files that are marked for universal compaction.
	p.computeFilesForUniversalCompaction()

	return p.pickPeriodicCompaction()

}

// Compute the sorted runs in the version associated with the
// compaction picker and store it in the associated state variable.
func (p *compactionPickerUniversal) computeSortedRuns(env compactionEnv) {
	p.sortedRuns = make([]sortedRunInfo, 0)

	// Iterate through all the files in L0. Each file in L0
	// is a sorted run.
	// TODO: Treat an L0 sub level as a single sorted run.
	iter := p.vers.Levels[0].Iter()
	for f := iter.First(); f != nil; f = iter.Next() {
		// Skip over files that are newer than earliestUnflushedSeqNum. This is
		// okay because this compaction can just pretend these files are not in
		// L0 yet.
		if f.LargestSeqNum < env.earliestUnflushedSeqNum {
			sortedRun := sortedRunInfo{
				level:               0,
				file:                f,
				size:                f.Size,
				compensatedFileSize: fileCompensation(f),
				beingCompacted:      f.IsCompacting(),
			}
			p.sortedRuns = append(p.sortedRuns, sortedRun)
		}

	}

	// Iterate through all the files in level > 0.
	// Files in each level form a single sorted run across the level.
	for i := 1; i < numLevels; i++ {
		lm := p.vers.Levels[i]
		totalCompensatedSize := levelCompensatedSize(lm)
		beingCompacted := false

		iter = p.vers.Levels[i].Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if f.IsCompacting() {
				// If any file in the sorted run is being compacted
				// we consider the whole sorted run to be compacted.
				beingCompacted = true
				break
			}
		}

		if totalCompensatedSize > 0 {
			sortedRun := sortedRunInfo{
				level:               i,
				file:                nil,
				size:                lm.Size(),
				compensatedFileSize: totalCompensatedSize,
				beingCompacted:      beingCompacted,
			}
			p.sortedRuns = append(p.sortedRuns, sortedRun)
		}

	}

}

// Mark the files for Universal compaction
func (p *compactionPickerUniversal) computeFilesForUniversalCompaction() {

	// [Q] Should we compute compaction scores?
	p.computeFilesMarkedForPeriodicCompaction()

}

// Mark the files for Periodic Universal Compaction and store it in the
// associated state variable in the compaction picker.
func (p *compactionPickerUniversal) computeFilesMarkedForPeriodicCompaction() {

	// For now, hardcode a value for the value for the periodic compaction
	// interval
	// [TODO] Add this to options
	periodicCompactionsSeconds := int64(3)

	// Clear the current slice of files
	p.filesMarkedForPeriodicCompaction = []levelFileMetadata{}

	if periodicCompactionsSeconds == 0 {
		return
	}

	currentTime := time.Now().Unix()
	if periodicCompactionsSeconds > currentTime {
		return
	}

	allowedTimeLimit := currentTime - periodicCompactionsSeconds

	// Check for files that were created before the allowed time limit.
	for i := 0; i < numLevels; i++ {
		iter := p.vers.Levels[i].Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if f.CreationTime < allowedTimeLimit && !f.IsCompacting() {
				fileWithLevel := levelFileMetadata{
					FileMetadata: f,
					level:        i,
				}
				p.filesMarkedForPeriodicCompaction = append(p.filesMarkedForPeriodicCompaction, fileWithLevel)
			}
		}

	}

}

// Pick and return a periodic compaction. Returns nil if no suitable compaction
// can be picked.
func (p *compactionPickerUniversal) pickPeriodicCompaction() (pc *pickedCompaction) {

	// In periodic universal compaction, sorted runs contain older data are almost always
	// generated earlier too and reside towards the bottom (higher level numbers)
	// of the LSM tree. To simplify the problem, we just try to trigger
	// a full compaction. We start from the last sorted run and include
	// all sorted runs, until we hit a sorted already being compacted.
	// Since usually the largest (which is usually the oldest) sorted run is
	// included anyway, doing a full compaction won't increase write
	// amplification much.

	// Start looking from the last sorted run till we find a sorted run
	// containing files that are already being compacted.
	startIndex := len(p.sortedRuns)
	for startIndex > 0 && !p.sortedRuns[startIndex-1].beingCompacted {
		startIndex--
	}

	// Even the very last sorted run is already being compacted.
	// We do not proceed with our compaction
	if startIndex == len(p.sortedRuns) {
		return nil
	}

	// Prior to this point, we have already marked files that are old enough
	// for periodic compaction. Generally older files would be at the bottom
	// of the LSM tree and would be part of the later sorted runs in our
	// list of sorted runs. But in rare corner cases, it is possible that
	// the files we marked for compaction are somewhere in the middle of
	// our sorted run list and since the above logic just picks sorted runs
	// at the end of the list, we may end up with a case, where the picked
	// compaction doesn't include any of the files we had marked for compaction.
	// Since this is rare, to keep the logic simple, If we have only one sorted
	// run to compact and none of the file to be compacted qualifies for
	// periodic compaction, skip the compact.

	// [Q] How exactly does this prevent compacting the same single level
	// over and over again?

	// Check if we have only 1 sorted run to compact
	if startIndex == len(p.sortedRuns)-1 {
		startLevel := p.sortedRuns[startIndex].level
		startFile := p.sortedRuns[startIndex].file

		// We have to iterate through the files picked for compaction
		// to see if final sorted run includes a file marked for compaction
		markedFileIncluded := false
		for _, lf := range p.filesMarkedForPeriodicCompaction {
			file := lf.FileMetadata
			level := lf.level

			// The last sorted run could be an L0 file or
			// an entire non-l0 level. We treat both
			// cases separately
			if startLevel != 0 {
				// Last sorted run is an entire non-l0 level
				if startLevel == level {
					markedFileIncluded = true
					break
				}
			} else {
				if startFile == file {
					markedFileIncluded = true
					break
				}
			}
		}

		if !markedFileIncluded {
			return nil
		}

	}

	return p.pickCompactionWithSortedRunRange(startIndex, len(p.sortedRuns)-1)

}

// Pick and return a compaction given the start and end indices of the sorted run list.
// Returns nil if a compaction could not be picked.
func (p *compactionPickerUniversal) pickCompactionWithSortedRunRange(startIndex, endIndex int) (pc *pickedCompaction) {

	startLevel := p.sortedRuns[startIndex].level
	if startLevel == numLevels-1 {
		// We don't support compactions originating from the last level
		// (Elision only compactions) for Universal compaction.
		return nil
	}

	// If we are compacting all the sorted runs, we just output it to the last
	// level. Otherwise we compact them into the highest level greater than or
	// equal to the highest level run being compacted but lower than the level
	// of the first sorted run that is not being compacted.
	// For instance say we are compacting sorted runs in L0, L1 and L2. L3 and
	// L4 are empty and L5 is a sorted run that is not being compacted,
	// we set the output level to L4.
	var outputLevel int
	if endIndex == len(p.sortedRuns)-1 {
		outputLevel = numLevels - 1
	} else {
		// If we are only compacting sorted runs in L0, but we cannot compact
		// all of them, we just skip the compaction. That is, we don't do
		// an intra L0 compaction.
		if p.sortedRuns[endIndex].level == 0 {
			return nil
		}
		outputLevel = p.sortedRuns[endIndex+1].level - 1
	}

	if startLevel > 0 && startLevel < p.baseLevel {
		panic(fmt.Sprintf("invalid compaction: start level %d should not be empty (base level %d)",
			startLevel, p.baseLevel))
	}

	adjustedLevel := adjustedOutputLevel(outputLevel, p.baseLevel)
	pc = &pickedCompaction{
		cmp:                    p.opts.Comparer.Compare,
		version:                p.vers,
		baseLevel:              p.baseLevel,
		maxOutputFileSize:      uint64(p.opts.Level(adjustedLevel).TargetFileSize),
		maxOverlapBytes:        maxGrandparentOverlapBytes(p.opts, adjustedLevel),
		maxReadCompactionBytes: maxReadCompactionBytes(p.opts, adjustedLevel),
	}

	// The files that we pick for compaction need to be placed in the inputs list.
	inputs := make([]compactionLevel, 0)

	// We iterate through the sorted runs between the start and end indices.
	// L0 files are treated separately. So we first iterate only as long
	// as we have sorted runs in L0. This is required because inputs is of
	// type []compaction level. Each compaction level object requires a level
	// number and a LevelSlice. However, the files that we pick for compaction
	// in L0 may not form a continuous slice within level 0. So we manually
	// create a LevelSlice with the files in L0 that we pick for compaction.
	i := startIndex
	firstFileAdded := false
	l0Files := make([]*manifest.FileMetadata, 0)

	for ; i <= endIndex && p.sortedRuns[i].level == 0; i++ {
		file := p.sortedRuns[i].file

		// Keep track of the key range bounds as we iterate through the files
		if !firstFileAdded {
			firstFileAdded = true
			pc.smallest, pc.largest = file.Smallest, file.Largest
		} else {
			if base.InternalCompare(p.opts.Comparer.Compare, pc.smallest, file.Smallest) >= 0 {
				pc.smallest = file.Smallest
			}
			if base.InternalCompare(p.opts.Comparer.Compare, pc.largest, file.Largest) <= 0 {
				pc.largest = file.Largest
			}
		}

		l0Files = append(l0Files, p.sortedRuns[i].file)
	}

	// Build the compactionLevel object for L0 files and add it to inputs
	if len(l0Files) > 0 {
		l0CompactionLevel := compactionLevel{
			level: 0,
			files: manifest.NewLevelSliceSeqSorted(l0Files),
		}
		l0CompactionLevel.l0SublevelInfo = generateSublevelInfo(pc.cmp, l0CompactionLevel.files)
		inputs = append(inputs, l0CompactionLevel)
	}

	// Proceed with the sorted runs in level > 0. Each level
	// is a sorted run and has a separate compactionLevel object
	levelIters := make([]manifest.LevelIterator, 0)
	for ; i <= endIndex; i++ {
		cl := compactionLevel{
			level: p.sortedRuns[i].level,

			// Since we consider the entire sorted run, we use
			// a slice that covers the whole level
			files: p.vers.Levels[p.sortedRuns[i].level].Slice(),
		}
		inputs = append(inputs, cl)
		levelIters = append(levelIters, p.vers.Levels[p.sortedRuns[i].level].Iter())
	}

	// Update the key range bounds based on the level > 0 files added
	if len(levelIters) > 0 {
		smallest, largest := manifest.KeyRange(p.opts.Comparer.Compare, levelIters...)
		if !firstFileAdded {
			firstFileAdded = true
			pc.smallest, pc.largest = smallest, largest
		} else {
			if base.InternalCompare(p.opts.Comparer.Compare, pc.smallest, smallest) >= 0 {
				pc.smallest = smallest
			}
			if base.InternalCompare(p.opts.Comparer.Compare, pc.largest, largest) <= 0 {
				pc.largest = largest
			}
		}
	}

	if len(inputs) == 0 {
		panic("invalid compaction: no input files to compact")
	}

	// If the output level that we calculated above is on a higher numbered level
	// than the last entry in inputs, then we add another entry for this level.
	// The 'files' field in this entry will be an empty slice.
	if outputLevel != inputs[len(inputs)-1].level {
		cl := compactionLevel{
			level: outputLevel,
		}
		inputs = append(inputs, cl)
	}

	// Set up the corresponding fields in the picked compaction object
	pc.inputs = inputs
	pc.startLevel = &pc.inputs[0]
	pc.outputLevel = &pc.inputs[len(inputs)-1]

	// If we end up picking sorted runs across 3 or more levels
	// we have to set the extra levels field.
	if len(inputs) > 2 {
		extraLevels := make([]*compactionLevel, 0)

		for i := 1; i <= len(inputs)-2; i++ {
			extraLevels = append(extraLevels, &inputs[i])
		}
		pc.extraLevels = extraLevels
	}

	pc.kind = compactionKindUniversalPeriodic

	// TODO: Verify if we need to set any of these fields to be set
	// We are not setting the following fields because they don't seem to
	// be used outside the picker logic
	// pc.score, pc.lcf
	// pc.pickerMetrics is used in compaction.go, but it seems to be
	// used just for information purposes and we would just see zero
	// values for its fields.

	return pc

}
