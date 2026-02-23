-- create table _prisma_migrations
-- (
--     id                  varchar(36)                            not null
--         primary key,
--     checksum            varchar(64)                            not null,
--     finished_at         timestamp with time zone,
--     migration_name      varchar(255)                           not null,
--     logs                text,
--     rolled_back_at      timestamp with time zone,
--     started_at          timestamp with time zone default now() not null,
--     applied_steps_count integer                  default 0     not null
-- );

-- alter table _prisma_migrations
--     owner to postgres;

create table double_gw
(
    id                   serial primary key,
    home         text                                                  not null,
    away         text                                                  not null,
    gw           integer                                               not null,
    original_gw  integer                                               not null,
    understat_id integer                                               not null,
    season       text                                                  not null
);

alter table double_gw
    owner to postgres;

create table fixtures
(
    id                   serial primary key,
    season                   text                                                 not null,
    gw                       integer                                              not null,
    fpl_element              integer                                              not null,
    local_understat_id       double precision,
    local_understat_fixture  double precision,
    fpl_name                 text                                                 not null,
    understat_name           text,
    position                 integer                                              not null,
    fpl_team                 integer                                              not null,
    understat_team           text,
    opponent_fpl_team_number integer                                              not null,
    fpl_kickoff_time         timestamp(3)                                         not null,
    understat_date           timestamp(3),
    value                    integer                                              not null,
    points                   integer                                              not null,
    minutes                  integer                                              not null,
    goals_scored             integer                                              not null,
    "xG"                     double precision                                     not null,
    goals_conceded           integer                                              not null,
    assists                  integer                                              not null,
    "xA"                     double precision                                     not null,
    yellow_cards             integer                                              not null,
    red_cards                integer                                              not null,
    clean_sheets             integer                                              not null,
    key_passes               double precision                                     not null,
    own_goals                integer                                              not null,
    penalties_missed         integer                                              not null,
    penalties_saved          integer                                              not null,
    saves                    integer                                              not null,
    bonus                    integer                                              not null,
    team_a_score             integer                                              not null,
    team_h_score             integer                                              not null,
    was_home                 boolean                                              not null,
    expected_assists         double precision,
    expected_goals           double precision
);

alter table fixtures
    owner to postgres;

create table games
(
    id                   serial primary key,
    understat_id         integer                                           not null,
    date                 text                                              not null,
    home                 text                                              not null,
    gw                   integer                                           not null,
    home_goals           double precision                                  not null,
    "home_xG"            double precision                                  not null,
    home_assists         double precision                                  not null,
    "home_xA"            double precision                                  not null,
    rolling_home_goals   double precision                                  not null,
    "rolling_home_xG"    double precision                                  not null,
    rolling_home_assists double precision                                  not null,
    "rolling_home_xA"    double precision                                  not null,
    home_team_code       integer                                           not null,
    away                 text                                              not null,
    away_goals           double precision                                  not null,
    "away_xG"            double precision                                  not null,
    away_assists         double precision                                  not null,
    "away_xA"            double precision                                  not null,
    rolling_away_goals   double precision                                  not null,
    "rolling_away_xG"    double precision                                  not null,
    rolling_away_assists double precision                                  not null,
    "rolling_away_xA"    double precision                                  not null,
    away_team_code       integer                                           not null,
    result               double precision                                  not null
);

alter table games
    owner to postgres;

create table players
(
    fpl_name       text             not null,
    understat_name text,
    fpl_202425     double precision not null,
    opta_id        integer,
    id             serial
        primary key
);

alter table players
    owner to postgres;

create unique index players_fpl_name_key
    on players (fpl_name);

create table predictions
(
    id                   serial primary key,
    understat_name            text                                                    not null,
    opta_id                   integer                                                 not null,
    gw                        integer                                                 not null,
    opponent_team             integer                                                 not null,
    global_predicted_points   double precision                                        not null,
    opponent_predicted_points double precision                                        not null,
    combined_predicted_points double precision                                        not null
);

alter table predictions
    owner to postgres;

create table teams
(
    season               text    not null,
    team                 integer not null,
    team_name            text    not null,
    definite_team_number integer not null,
    understat_name       text    not null,
    id                   serial
        primary key
);

alter table teams
    owner to postgres;

