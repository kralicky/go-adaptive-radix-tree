package art

import "bytes"

func DelimiterResolver(delim byte) func(key Key, conflictIndex int) (Key, int, int) {
	return func(key Key, conflictIndex int) (Key, int, int) {
		if conflictIndex > len(key) {
			return nil, -1, -1
		}
		upperBoundOffset := 1
		if conflictIndex < len(key) {
			// one of the keys is larger than the other,
			c := key[conflictIndex]
			switch c {
			case '*':
				return nil, -1, -1
			case delim:
				// if the conflict is on a delimiter, shift to the left, ex:
				// tree has "123.456" and "*.456"
				// input is "12.456"
				//             ^ conflict here
				upperBoundOffset = 0
			}
		} else {
			upperBoundOffset = 0
		}
		lowerBound := bytes.LastIndexByte(key[:conflictIndex+upperBoundOffset], delim) + 1
		upperBound := bytes.IndexByte(key[conflictIndex:], delim)
		if upperBound == -1 {
			upperBound = len(key)
		} else {
			upperBound = conflictIndex + upperBound
		}
		return Key("*"), lowerBound, upperBound
	}
}
