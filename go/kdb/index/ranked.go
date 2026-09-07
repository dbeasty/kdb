package index

import "sort"

func sortRanked(results []RankedResult) {
	sort.SliceStable(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].DocID.String() < results[j].DocID.String()
	})
}
