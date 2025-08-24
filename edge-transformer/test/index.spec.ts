import { env, createExecutionContext, waitOnExecutionContext, SELF } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';
import worker from '../src/index';

// For now, you'll need to do something like this to get a correctly-typed
// `Request` to pass to `worker.fetch()`.
const IncomingRequest = Request<unknown, IncomingRequestCfProperties>;

const samplePayload = {
	caseNumber: 4,
	users: [
		{
			Id: 1,
			Reputation: 150,
			CreationDate: '2020-01-01T00:00:00Z',
			DisplayName: 'Alice Example',
			LastAccessDate: '2025-01-01T00:00:00Z',
			WebsiteUrl: null,
			Location: 'Springfield, USA',
			AboutMe: 'Hello, <b>world</b>!',
			Views: 100,
			UpVotes: 10,
			DownVotes: 2,
		},
	],
};

describe('Edge transformer worker', () => {
	it('processes POST requests with user payload (unit style)', async () => {
		const request = new IncomingRequest('http://example.com', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(samplePayload),
		});

		const ctx = createExecutionContext();
		const response = await worker.fetch(request, env, ctx);
		await waitOnExecutionContext(ctx);

		expect(response.status).toBe(200);
		const json = (await response.json()) as any;
		expect(json.processed).toBe(1);
		expect(Array.isArray(json.transformedUsers)).toBe(true);
		expect(json.transformedUsers[0]).toHaveProperty('user_id', 1);
	});

	it('processes POST requests with user payload (integration style)', async () => {
		const response = await SELF.fetch('https://example.com', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(samplePayload),
		});
		expect(response.status).toBe(200);
		const json = (await response.json()) as any;
		expect(json.processed).toBe(1);
	});
});
