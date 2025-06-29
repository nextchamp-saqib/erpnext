import { createRouter, createWebHistory } from 'vue-router'
import session from './session'

const routes = [
	{
		path: '/',
		name: 'Home',
		component: () => import('@/pages/Home.vue'),
	},
]

let router = createRouter({
	history: createWebHistory('/projects'),
	routes,
})

router.beforeEach(async (to, _, next) => {
	if (!session.isLoggedIn) {
		window.location.href = '/login?redirect-to=/projects'
		return next(false)
	}
	return next()
})

export default router
