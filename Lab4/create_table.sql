CREATE TABLE `file` (
   `id` INTEGER PRIMARY KEY AUTOINCREMENT,
   `filename` TEXT,
   `size` INTEGER,
   `content_type` TEXT,
   `created` DATETIME DEFAULT CURRENT_TIMESTAMP,
   `part1_filenames` TEXT,
   `part2_filenames` TEXT
);
