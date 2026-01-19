type FeedbackItem = {
	ts: string;
	source: string;
	summary: string;
	product?: string;
	tags?: string[];
};

type AI = {
	run: (model: string, input: unknown) => Promise<unknown>;
};

type Env = {
	FEEDBACK_KV: KVNamespace;
	AI: AI;
};

const MAX_ITEMS_PER_INGEST = 50;
const MAX_ITEMS_FOR_INSIGHTS = 50;
const MIN_DAYS = 1;
const MAX_DAYS = 30;
const DEFAULT_DAYS = 7;
const MODEL = '@cf/meta/llama-3-8b-instruct';
const INSIGHTS_CACHE_KEY = 'insights:latest';

export default {
	async fetch(request, env): Promise<Response> {
		try {
			const url = new URL(request.url);
			if (request.method === 'GET' && url.pathname === '/') {
				return serveHtml();
			}
			if (request.method === 'POST' && url.pathname === '/ingest') {
				return handleIngest(request, env);
			}
			if (request.method === 'POST' && url.pathname === '/insights') {
				return handleInsights(request, env);
			}
			if (request.method === 'GET' && url.pathname === '/latest') {
				return handleLatest(env);
			}
			return jsonResponse({ ok: false, error: 'not_found' }, 404);
		} catch (err) {
			return jsonResponse(
				{ ok: false, error: 'internal_error', details: toErrorMessage(err) },
				500,
			);
		}
	},
} satisfies ExportedHandler<Env>;

async function handleIngest(request: Request, env: Env): Promise<Response> {
	const body = await safeParseJson(request);
	if (!body || !Array.isArray(body.items)) {
		return jsonResponse({ ok: false, error: 'invalid_body' }, 400);
	}
	const items: FeedbackItem[] = body.items;
	if (items.length === 0) {
		return jsonResponse({ ok: false, error: 'no_items' }, 400);
	}
	if (items.length > MAX_ITEMS_PER_INGEST) {
		return jsonResponse({ ok: false, error: 'too_many_items' }, 400);
	}

	const dayCounts = new Map<string, number>();
	let ingested = 0;

	for (const item of items) {
		if (!isValidItem(item)) {
			return jsonResponse({ ok: false, error: 'invalid_item' }, 400);
		}
		const day = isoDay(item.ts);
		const countKey = `day:${day}:count`;
		let currentCount: number;
		if (dayCounts.has(day)) {
			currentCount = dayCounts.get(day)!;
		} else {
			const stored = await env.FEEDBACK_KV.get(countKey);
			currentCount = stored ? parseInt(stored, 10) || 0 : 0;
		}
		const next = currentCount + 1;
		dayCounts.set(day, next);

		const fbKey = `fb:${day}:${crypto.randomUUID()}`;
		const pointerKey = `day:${day}:item:${next}`;

		await env.FEEDBACK_KV.put(fbKey, JSON.stringify(item));
		await env.FEEDBACK_KV.put(pointerKey, fbKey);
		await env.FEEDBACK_KV.put(countKey, String(next));
		ingested += 1;
	}

	return jsonResponse({ ok: true, ingested });
}

async function handleInsights(request: Request, env: Env): Promise<Response> {
	const body = await safeParseJson(request);
	let days: number = body && typeof body.days === 'number' ? body.days : DEFAULT_DAYS;
	days = clamp(days, MIN_DAYS, MAX_DAYS);

	const today = new Date();
	const dayStrings: string[] = [];
	for (let i = 0; i < days; i++) {
		const d = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()));
		d.setUTCDate(d.getUTCDate() - i);
		dayStrings.push(d.toISOString().slice(0, 10));
	}

	const collected: FeedbackItem[] = [];

	for (const day of dayStrings) {
		if (collected.length >= MAX_ITEMS_FOR_INSIGHTS) break;
		const countStr = await env.FEEDBACK_KV.get(`day:${day}:count`);
		const count = countStr ? parseInt(countStr, 10) || 0 : 0;
		for (let n = count; n >= 1 && collected.length < MAX_ITEMS_FOR_INSIGHTS; n--) {
			const pointerKey = `day:${day}:item:${n}`;
			const fbKey = await env.FEEDBACK_KV.get(pointerKey);
			if (!fbKey) continue;
			const raw = await env.FEEDBACK_KV.get(fbKey);
			if (!raw) continue;
			const parsed = safeParseJsonString(raw);
			if (parsed && isValidItem(parsed)) {
				collected.push(parsed);
			}
		}
	}

	const aiInput = buildAiInput(collected);
	const aiPrompt = buildAiPrompt(aiInput);
	let aiResponseText = '';
	try {
		const aiResult = await env.AI.run(MODEL, {
			messages: [
				{
					role: 'system',
					content:
						'You summarize product feedback. Output JSON only, matching the provided schema, no prose.',
				},
				{
					role: 'user',
					content: aiPrompt,
				},
			],
			stream: false,
		});
		aiResponseText = extractAiText(aiResult);
	} catch (err) {
		return jsonResponse(
			{ ok: false, error: 'ai_error', details: toErrorMessage(err) },
			502,
		);
	}

	const parsedInsights = safeParseJsonString(aiResponseText);
	if (!parsedInsights) {
		return jsonResponse(
			{ ok: false, error: 'invalid_ai_response', raw: aiResponseText },
			502,
		);
	}

	const generatedAt = new Date().toISOString();
	const range = {
		days,
		from: dayStrings[dayStrings.length - 1],
		to: dayStrings[0],
	};

	const result = {
		ok: true,
		range,
		input: { items_considered: collected.length },
		insights: parsedInsights,
		generated_at: generatedAt,
	};

	await env.FEEDBACK_KV.put(INSIGHTS_CACHE_KEY, JSON.stringify(result));
	return jsonResponse(result);
}

async function handleLatest(env: Env): Promise<Response> {
	const cached = await env.FEEDBACK_KV.get(INSIGHTS_CACHE_KEY);
	if (!cached) {
		return jsonResponse({ ok: false, error: 'no_cached_insights' }, 404);
	}
	const parsed = safeParseJsonString(cached);
	if (!parsed) {
		return jsonResponse({ ok: false, error: 'cache_corrupt' }, 500);
	}
	return jsonResponse(parsed);
}

function serveHtml(): Response {
	const example = JSON.stringify(
		{
			items: [
				{
					ts: new Date().toISOString(),
					source: 'support',
					summary: 'Customers want clearer error messages.',
					product: 'Workers',
					tags: ['dx', 'errors'],
				},
			],
		},
		null,
		2,
	);

	const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Feedback Insights</title>
  <style>
    body { font-family: system-ui, sans-serif; padding: 16px; max-width: 900px; margin: 0 auto; }
    textarea { width: 100%; height: 200px; font-family: monospace; }
    button { margin-right: 8px; margin-top: 8px; }
    pre { background: #f5f5f5; padding: 12px; border-radius: 6px; white-space: pre-wrap; }
    input[type="number"] { width: 80px; }
  </style>
</head>
<body>
  <h1>Feedback Insights Worker</h1>
  <p>Endpoints: POST /ingest, POST /insights, GET /latest</p>
  <label for="payload">/ingest JSON payload:</label>
  <textarea id="payload">${escapeHtml(example)}</textarea>
  <div>
    <button id="btnIngest">POST /ingest</button>
    <input id="days" type="number" min="1" max="30" value="7" />
    <button id="btnInsights">POST /insights</button>
    <button id="btnLatest">GET /latest</button>
  </div>
  <pre id="output">Ready.</pre>
  <script>
    const out = document.getElementById('output');
    const setOut = (v) => out.textContent = v;

    document.getElementById('btnIngest').onclick = async () => {
      try {
        const body = document.getElementById('payload').value;
        const res = await fetch('/ingest', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
        setOut(await res.text());
      } catch (e) { setOut(String(e)); }
    };

    document.getElementById('btnInsights').onclick = async () => {
      try {
        const days = Number(document.getElementById('days').value || 7);
        const res = await fetch('/insights', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ days }) });
        setOut(await res.text());
      } catch (e) { setOut(String(e)); }
    };

    document.getElementById('btnLatest').onclick = async () => {
      try {
        const res = await fetch('/latest');
        setOut(await res.text());
      } catch (e) { setOut(String(e)); }
    };
  </script>
</body>
</html>`;

	return new Response(html, {
		headers: { 'content-type': 'text/html; charset=utf-8' },
	});
}

function jsonResponse(data: unknown, status = 200): Response {
	return new Response(JSON.stringify(data), {
		status,
		headers: { 'content-type': 'application/json' },
	});
}

async function safeParseJson(request: Request): Promise<any | null> {
	try {
		return await request.json();
	} catch {
		return null;
	}
}

function safeParseJsonString(value: string): any | null {
	try {
		return JSON.parse(value);
	} catch {
		return null;
	}
}

function isValidItem(item: any): item is FeedbackItem {
	return (
		item &&
		typeof item.ts === 'string' &&
		isValidDate(item.ts) &&
		typeof item.source === 'string' &&
		typeof item.summary === 'string'
	);
}

function isValidDate(v: string): boolean {
	const d = new Date(v);
	return !Number.isNaN(d.valueOf());
}

function isoDay(ts: string): string {
	return new Date(ts).toISOString().slice(0, 10);
}

function clamp(n: number, min: number, max: number): number {
	return Math.max(min, Math.min(max, n));
}

function buildAiInput(items: FeedbackItem[]): string {
	const lines = items.map((it) => {
		const parts = [
			`ts=${it.ts}`,
			`source=${it.source}`,
			it.product ? `product=${it.product}` : '',
			it.tags && it.tags.length ? `tags=${it.tags.join('|')}` : '',
			`summary=${it.summary}`,
		].filter(Boolean);
		return parts.join(' | ');
	});
	return lines.join('\n');
}

function buildAiPrompt(raw: string): string {
	return [
		'You are a product feedback synthesizer.',
		'Given recent aggregated feedback lines, produce JSON ONLY with this exact shape:',
		'{',
		'  "top_themes": [{ "theme": string, "evidence_count": number }],',
		'  "risks": [string],',
		'  "recommended_actions": [string],',
		'  "open_questions": [string]',
		'}',
		'Do not add any fields. Do not add prose outside the JSON. Avoid repetition. If no data, return empty arrays.',
		'Feedback lines:',
		raw || '(no feedback)',
	].join('\n');
}

function escapeHtml(value: string): string {
	return value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function extractAiText(aiResult: any): string {
	if (!aiResult) return '';
	if (typeof aiResult === 'string') return aiResult;
	if ('response' in aiResult && typeof aiResult.response === 'string') return aiResult.response;
	if ('result' in aiResult && typeof aiResult.result === 'string') return aiResult.result;
	return JSON.stringify(aiResult);
}

function toErrorMessage(err: unknown): string {
	if (err instanceof Error) return err.message;
	return String(err);
}
