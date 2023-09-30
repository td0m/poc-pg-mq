create extension tsm_system_rows;

create table if not exists tasks(
	_id bigint primary key generated always as identity,
	name text not null,
	data jsonb not null default '{}',
	created_at timestamptz not null default now(),
	unique(name, data) -- pseudo idempotency, optional
);
