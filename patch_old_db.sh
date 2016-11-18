sqlite3 ~/.tagman/default.sql "ALTER TABLE resource_tag ADD COLUMN tag_path TEXT; \
								UPDATE resource_tag SET tag_path = (SELECT replace(name,' ','-')||'-' FROM tag WHERE tag.rowid=tag_id); \
								CREATE INDEX resource_tag_by_tagpath on resource_tag (tag_path); \
								CREATE TABLE meta (key TEXT, value TEXT); \
								INSERT INTO meta (key,value) VALUES ('format_version','1');"

