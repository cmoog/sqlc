CREATE TABLE students (
  id int,
  school_id VARCHAR(255),
  school_lat FLOAT,
  PRIMARY KEY (ID)
);

CREATE TABLE teachers ( 
  id int,
  school_id VARCHAR(255)
)

/* name: GetAllStudents :many */
SELECT school_id, id FROM students WHERE id = ? AND school_id = ?

/* name: GetSomeStudents :one */
SELECT school_id, id FROM students WHERE school_id = ?

/* name: StudentByID :one */
SELECT id, school_lat FROM students WHERE id = ? LIMIT 10