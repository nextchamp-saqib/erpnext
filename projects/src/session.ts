import { call } from 'frappe-ui'
import { computed, reactive, ref } from 'vue'

const initialized = ref(false)

const user = ref({
	email: '',
	first_name: '',
	last_name: '',
	full_name: '',
	user_image: '',
	country: '',
	locale: 'en-US',
})

const isLoggedIn = computed(() => {
	return user.value.email && user.value.email !== 'Guest'
})

async function initialize() {
	Object.assign(user.value, getSessionFromCookies())
	initialized.value = true
}

async function login(email: string, password: string) {
	reset()
	const res = await call('login', { usr: email, pwd: password })
	if (!res) {
		throw new Error('Login failed')
	}
	Object.assign(user.value, {
		email: res.email,
		first_name: res.first_name,
		last_name: res.last_name,
		full_name: res.full_name,
		user_image: res.user_image,
		country: res.country,
		locale: res.locale || 'en-US',
	})
	window.location.reload()
}

async function logout() {
	reset()
	await call('logout')
	window.location.reload()
}

function reset() {
	Object.assign(user.value, {
		email: '',
		first_name: '',
		last_name: '',
		full_name: '',
		user_image: '',
		country: '',
		locale: 'en-US',
	})
}

function getSessionFromCookies() {
	return document.cookie
		.split('; ')
		.map((c) => c.split('='))
		.reduce((acc, [key, value]) => {
			key = key === 'user_id' ? 'email' : key
			acc[key] = decodeURIComponent(value)
			return acc
		}, {} as any)
}

initialize().catch((err) => {
	console.error('Failed to initialize session:', err)
})

export default reactive({
	initialized,
	user,
	isLoggedIn,
	initialize,
	login,
	logout,
	reset,
})
