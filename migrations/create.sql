CREATE TABLE IF NOT EXISTS `sockpuppet_service`.`user` (
        id INTEGER NOT NULL AUTO_INCREMENT,
        user_text VARCHAR(255) NOT NULL,
        is_anon BOOLEAN NOT NULL,
        num_edits INTEGER,
        num_pages INTEGER,
        most_recent_edit DATETIME,
        oldest_edit DATETIME,
        insertion_time DATETIME DEFAULT (CURRENT_TIMESTAMP),
        dataset_id VARCHAR(36),
        PRIMARY KEY (id),
        CHECK (is_anon IN (0, 1))
);

CREATE INDEX `ix_user_user_text` ON `sockpuppet_service`.`user` (user_text);


CREATE TABLE IF NOT EXISTS `sockpuppet_service`.`coedit` (
        id INTEGER NOT NULL AUTO_INCREMENT,
        user_text VARCHAR(255) NOT NULL,
        user_text_neighbour VARCHAR(255) NOT NULL,
        overlap_count INTEGER NOT NULL,
        insertion_time DATETIME DEFAULT (CURRENT_TIMESTAMP),
        dataset_id VARCHAR(36),
        PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS `ix_coedit_user_text` ON `sockpuppet_service`.`coedit` (user_text);

CREATE TABLE IF NOT EXISTS `sockpuppet_service`.`temporal` (
        id INTEGER NOT NULL AUTO_INCREMENT,
        user_text VARCHAR(255) NOT NULL,
        d TINYINT NOT NULL,
        h TINYINT NOT NULL,
        num_edits INTEGER NOT NULL,
        insertion_time DATETIME DEFAULT CURRENT_TIMESTAMP,
        dataset_id VARCHAR(36),
        PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS `service_ix_temporal_user_text` ON `sockpuppet_service`.`temporal` (user_text);