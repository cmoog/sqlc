CREATE TABLE students (
  id int,
  school_id VARCHAR(255),
  school_lat VARCHAR(255),
  PRIMARY KEY (ID)
);

/* name: GetAllStudents :many */
SELECT school_id, id, school_id FROM students WHERE id = :id + ?

/* name: GetAllStudents :many */
SELECT school_id, id, school_id FROM students WHERE id = :id + ?