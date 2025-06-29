import { ref } from "vue";

type Breadcrumb = {
	label: string;
	route: string;
}

export const breadcrumbs = ref<Breadcrumb[]>([]);

export function setBreadcrumbs(crumbs: Breadcrumb[]) {
	breadcrumbs.value = crumbs;
}
