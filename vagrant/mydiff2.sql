create table A (
    id integer auto_increment
,   name varchar(64) not null

,   primary key(id)
,   unique(name)
);

insert into A (name) values ('hi');
insert into A (name) values ('GREETINGS');
insert into A (name) values ('hello');

delete from A where name = 'hi';

create table z (a int, b int, name varchar(64), primary key(a, b), unique(name));
create table y (id int auto_increment, a int, b int, c int, primary key(id), constraint fk_y_z foreign key(a,b) references z(a,b));
