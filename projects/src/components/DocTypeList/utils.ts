
export function pluralize(word: string) {
	if (word.endsWith('s')) {
		return word
	}
	return `${word}s`
}
