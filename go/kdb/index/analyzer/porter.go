package analyzer

// Porter stemmer: the original 1980 algorithm (M. F. Porter, "An algorithm for suffix
// stripping", Program 14(3)). This is a transcription of the reference implementation's
// control flow with the paper's rule tables - in particular step 2 keeps the paper's
// ABLI → ABLE and has no LOGI → LOG rule, which later "departures" in the Tartarus code
// added. Both trees implement exactly this variant; porter_vectors.txt pins it.

type stemmer struct {
	b []byte
	k int // index of the last letter
	j int // scratch: end of the stem for the current rule
}

// Stem returns the Porter stem of a lowercased token. Tokens of two letters or fewer are
// returned unchanged (the reference does the same), as is any token containing a non-ASCII
// byte: the algorithm is defined over ASCII letters only, so a token with any other letter is
// left alone rather than mangled.
func Stem(word string) string {
	if len(word) <= 2 {
		return word
	}
	for i := 0; i < len(word); i++ {
		if word[i] >= 0x80 {
			return word
		}
	}
	z := &stemmer{b: []byte(word), k: len(word) - 1}
	z.step1ab()
	z.step1c()
	z.step2()
	z.step3()
	z.step4()
	z.step5()
	return string(z.b[:z.k+1])
}

// cons reports whether b[i] is a consonant. 'y' is a consonant at the start of a word and
// after a vowel; otherwise it is a vowel.
func (z *stemmer) cons(i int) bool {
	switch z.b[i] {
	case 'a', 'e', 'i', 'o', 'u':
		return false
	case 'y':
		if i == 0 {
			return true
		}
		return !z.cons(i - 1)
	default:
		return true
	}
}

// m measures the number of consonant sequences between 0 and j: [c](vc){m}[v].
func (z *stemmer) m() int {
	n := 0
	i := 0
	for {
		if i > z.j {
			return n
		}
		if !z.cons(i) {
			break
		}
		i++
	}
	i++
	for {
		for {
			if i > z.j {
				return n
			}
			if z.cons(i) {
				break
			}
			i++
		}
		i++
		n++
		for {
			if i > z.j {
				return n
			}
			if !z.cons(i) {
				break
			}
			i++
		}
		i++
	}
}

// vowelInStem reports whether 0..j contains a vowel.
func (z *stemmer) vowelInStem() bool {
	for i := 0; i <= z.j; i++ {
		if !z.cons(i) {
			return true
		}
	}
	return false
}

// doubleC reports whether j and j-1 are the same consonant.
func (z *stemmer) doubleC(j int) bool {
	if j < 1 {
		return false
	}
	if z.b[j] != z.b[j-1] {
		return false
	}
	return z.cons(j)
}

// cvc reports whether i-2, i-1, i is consonant-vowel-consonant with the second consonant not
// w, x or y - the shape whose final e is restored (hop-ing → hope).
func (z *stemmer) cvc(i int) bool {
	if i < 2 || !z.cons(i) || z.cons(i-1) || !z.cons(i-2) {
		return false
	}
	switch z.b[i] {
	case 'w', 'x', 'y':
		return false
	}
	return true
}

// ends reports whether the word ends with s and, if so, sets j to the index before it.
func (z *stemmer) ends(s string) bool {
	l := len(s)
	if l > z.k+1 {
		return false
	}
	if string(z.b[z.k-l+1:z.k+1]) != s {
		return false
	}
	z.j = z.k - l
	return true
}

// setTo replaces the suffix after j with s.
func (z *stemmer) setTo(s string) {
	z.b = append(z.b[:z.j+1], s...)
	z.k = z.j + len(s)
}

func (z *stemmer) r(s string) {
	if z.m() > 0 {
		z.setTo(s)
	}
}

func (z *stemmer) step1ab() {
	if z.b[z.k] == 's' {
		switch {
		case z.ends("sses"):
			z.k -= 2
		case z.ends("ies"):
			z.setTo("i")
		case z.b[z.k-1] != 's':
			z.k--
		}
	}
	if z.ends("eed") {
		if z.m() > 0 {
			z.k--
		}
	} else if (z.ends("ed") || z.ends("ing")) && z.vowelInStem() {
		z.k = z.j
		switch {
		case z.ends("at"):
			z.setTo("ate")
		case z.ends("bl"):
			z.setTo("ble")
		case z.ends("iz"):
			z.setTo("ize")
		case z.doubleC(z.k):
			z.k--
			switch z.b[z.k] {
			case 'l', 's', 'z':
				z.k++
			}
		default:
			z.j = z.k
			if z.m() == 1 && z.cvc(z.k) {
				z.setTo("e")
			}
		}
	}
}

func (z *stemmer) step1c() {
	if z.ends("y") && z.vowelInStem() {
		z.b[z.k] = 'i'
	}
}

func (z *stemmer) step2() {
	if z.k < 1 {
		return
	}
	switch z.b[z.k-1] {
	case 'a':
		if z.ends("ational") {
			z.r("ate")
		} else if z.ends("tional") {
			z.r("tion")
		}
	case 'c':
		if z.ends("enci") {
			z.r("ence")
		} else if z.ends("anci") {
			z.r("ance")
		}
	case 'e':
		if z.ends("izer") {
			z.r("ize")
		}
	case 'l':
		switch {
		case z.ends("abli"):
			z.r("able")
		case z.ends("alli"):
			z.r("al")
		case z.ends("entli"):
			z.r("ent")
		case z.ends("eli"):
			z.r("e")
		case z.ends("ousli"):
			z.r("ous")
		}
	case 'o':
		switch {
		case z.ends("ization"):
			z.r("ize")
		case z.ends("ation"):
			z.r("ate")
		case z.ends("ator"):
			z.r("ate")
		}
	case 's':
		switch {
		case z.ends("alism"):
			z.r("al")
		case z.ends("iveness"):
			z.r("ive")
		case z.ends("fulness"):
			z.r("ful")
		case z.ends("ousness"):
			z.r("ous")
		}
	case 't':
		switch {
		case z.ends("aliti"):
			z.r("al")
		case z.ends("iviti"):
			z.r("ive")
		case z.ends("biliti"):
			z.r("ble")
		}
	}
}

func (z *stemmer) step3() {
	switch z.b[z.k] {
	case 'e':
		switch {
		case z.ends("icate"):
			z.r("ic")
		case z.ends("ative"):
			z.r("")
		case z.ends("alize"):
			z.r("al")
		}
	case 'i':
		if z.ends("iciti") {
			z.r("ic")
		}
	case 'l':
		if z.ends("ical") {
			z.r("ic")
		} else if z.ends("ful") {
			z.r("")
		}
	case 's':
		if z.ends("ness") {
			z.r("")
		}
	}
}

func (z *stemmer) step4() {
	if z.k < 1 {
		return
	}
	matched := false
	switch z.b[z.k-1] {
	case 'a':
		matched = z.ends("al")
	case 'c':
		matched = z.ends("ance") || z.ends("ence")
	case 'e':
		matched = z.ends("er")
	case 'i':
		matched = z.ends("ic")
	case 'l':
		matched = z.ends("able") || z.ends("ible")
	case 'n':
		matched = z.ends("ant") || z.ends("ement") || z.ends("ment") || z.ends("ent")
	case 'o':
		if z.ends("ion") && z.j >= 0 && (z.b[z.j] == 's' || z.b[z.j] == 't') {
			matched = true
		} else {
			matched = z.ends("ou")
		}
	case 's':
		matched = z.ends("ism")
	case 't':
		matched = z.ends("ate") || z.ends("iti")
	case 'u':
		matched = z.ends("ous")
	case 'v':
		matched = z.ends("ive")
	case 'z':
		matched = z.ends("ize")
	}
	if matched && z.m() > 1 {
		z.k = z.j
	}
}

func (z *stemmer) step5() {
	z.j = z.k
	if z.b[z.k] == 'e' {
		a := z.m()
		if a > 1 || (a == 1 && !z.cvc(z.k-1)) {
			z.k--
		}
	}
	if z.b[z.k] == 'l' && z.doubleC(z.k) && z.m() > 1 {
		z.k--
	}
}
