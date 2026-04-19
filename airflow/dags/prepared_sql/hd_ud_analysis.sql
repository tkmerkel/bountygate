SELECT * FROM hd_ud_analysis
where update_time > NOW() - INTERVAL '2 DAY';
