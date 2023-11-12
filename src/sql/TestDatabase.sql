
SELECT name FROM master.dbo.sysdatabases;

select * from boardgamepublisher

select count(game_id) as game_published,boardgamepublisher_name from bridge_boardgamepublisher brige_bga
inner join boardgamepublisher bga on bga.boardgamepublisher_id=brige_bga.boardgamepublisher_id
group by brige_bga.boardgamepublisher_id,boardgamepublisher_name

