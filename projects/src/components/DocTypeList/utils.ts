export const dataFields = [
	'Text',
	'Small Text',
	'Text Editor',
	'HTML Editor',
	'Data',
	'Code',
	'Phone',
	'JSON',
	'Read Only',
]

export const no_value_type = [
	'Section Break',
	'Column Break',
	'Tab Break',
	'HTML',
	'Table',
	'Table MultiSelect',
	'Button',
	'Image',
	'Fold',
	'Heading',
]

export function isValueType(fieldtype: string) {
	return no_value_type.indexOf(fieldtype) === -1
}

export function hasPerm(permlevel?: number) {
	return true
}

export function pluralize(str: string) {
	if (str.endsWith('s')) {
		return str
	}
	return str + 's'
}
