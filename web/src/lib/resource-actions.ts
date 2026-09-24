// Kept apart from `resources.ts`, which imports browser API helpers, so node tests can load it

import { Schema } from 'effect';

const QueuePlacementSchema = Schema.Union(
	Schema.Struct({ type: Schema.Literal('front') }),
	Schema.Struct({ type: Schema.Literal('back') }),
	Schema.Struct({ type: Schema.Literal('before'), request_id: Schema.String }),
	Schema.Struct({ type: Schema.Literal('after'), request_id: Schema.String })
);

/** Schema of the operations permitted from the browser dashboard. */
export const BrowserResourceActionSchema = Schema.Union(
	Schema.Struct({ type: Schema.Literal('cancel_queued'), request_id: Schema.String }),
	Schema.Struct({
		type: Schema.Literal('move_queued'),
		request_id: Schema.String,
		placement: QueuePlacementSchema
	}),
	Schema.Struct({ type: Schema.Literal('stop_active'), task_id: Schema.String }),
	Schema.Struct({ type: Schema.Literal('renotify'), notice_id: Schema.String })
);

/** Place for a queued request relative to the other queued requests. */
export type QueuePlacement = Schema.Schema.Type<typeof QueuePlacementSchema>;

/** The operations permitted from the browser dashboard. */
export type BrowserResourceAction = Schema.Schema.Type<typeof BrowserResourceActionSchema>;
