create table A (
    id integer auto_increment
,   name varchar(64) not null

,   primary key(id)
,   unique(name)
);

insert into A (name) values ('hi');
insert into A (name) values ('greetings');
