<template>
	<div class="max-w-3xl py-12 mx-auto">
		<Button icon-left="code" @click="ping.fetch" :loading="ping.loading">
			Click to send 'ping' request
		</Button>
		<div>
			{{ ping.data }}
		</div>
		<pre>{{ ping }}</pre>

		<Button @click="showDialog = true">Open Dialog</Button>
		<Dialog
			v-model="showDialog"
			:options="{
				title: 'Dialog Title',
			}"
		/>
	</div>
</template>

<script setup lang="ts">
import { Dialog } from 'frappe-ui'
import { createResource } from 'frappe-ui'
import { ref } from 'vue'

const showDialog = ref(false)
const ping = createResource({ url: 'ping' })
</script>
