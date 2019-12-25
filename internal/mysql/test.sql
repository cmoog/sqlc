CREATE TABLE students (
  id int,
  school_id VARCHAR(255),
  school_lat VARCHAR(255),
  PRIMARY KEY (ID)
);

CREATE TABLE teachers ( 
  id int,
  school_id VARCHAR(255)
)

/* name: GetAllStudents :many */
SELECT school_id, id FROM students WHERE id = :id + ?

/* name: GetSomeStudents :many */
SELECT school_id, id FROM students WHERE id < ?