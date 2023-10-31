package impl

import "crypto"

// SHA256 hashes a byte-formed data stream, returns the calculated hash
func SHA256(buf []byte) []byte {
	hash := crypto.SHA256.New()
	hash.Write(buf)
	return hash.Sum(nil)
}

// removeDupValues return a string slice without duplicate values
func removeDupValues(slice []string) []string {
	var list []string
	for _, val := range slice {
		exist := false
		for _, val2 := range list {
			if val == val2 {
				exist = true
				break
			}
		}
		if !exist {
			list = append(list, val)
		}
	}
	return list
}
