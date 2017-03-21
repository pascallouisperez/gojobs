CREATE TABLE job_queue (
  id              bigint not null auto_increment,
  created_at      bigint not null,

  name            varchar(255) not null,
  params          blob null,

  remaining       int not null,
  schedulable_at  bigint null,
  processor_id    bigint null,
  status          enum('pending', 'processing', 'failed', 'completed'),

  primary key (id)
);
