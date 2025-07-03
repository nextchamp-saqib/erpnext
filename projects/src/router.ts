import { usePageMeta } from 'frappe-ui'
import { session } from 'frappe-ui/frappe'
import { createRouter, createWebHistory } from 'vue-router'

const routes = [
	{
		path: '/',
		component: () => import('@/pages/ListView.vue'),
	},
	{
		props: true,
		path: '/:doctype',
		name: 'ListView',
		component: () => import('@/pages/ListView.vue'),
	}
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

	if (typeof to.name === 'string') {
		const title = to.name
		usePageMeta(() => ({ title }))
	}

	return next()
})

export default router
