-- Populate Instructors table
DELETE FROM INSTRUCTORS;
INSERT INTO INSTRUCTORS (InstructorID, InstructorName)
VALUES
    (2, 'Dr. Brown'),
    (3, 'Dr. Smith'),
    (4, 'Dr. White');

-- Populate Students table
DELETE FROM STUDENTS;
INSERT INTO STUDENTS (StudentID, FirstName, LastName, Email, Major, AdvisorID)
VALUES
    (1, 'John', 'Doe', 'john.doe@example.com', 'Computer Science', 3),
    (2, 'Jane', 'Doe', 'jane.doe@example.com', 'Business', 2),
    (3, 'Jim', 'Beam', 'jim.beam@example.com', 'Mathematics', 3),
    (4, 'Alice', 'Johnson', 'alice.johnson@example.com', 'Computer Science', 4),
    (5, 'John', 'Smith', 'john.smith@example.com', 'Business', 2),
    (6, 'Bill', 'Chu', 'bill.chu@example.com', 'Mathematics', 3),
    (7, 'David', 'Small', 'david.small@example.com', 'Computer Science', 3);

-- Populate Courses table
DELETE FROM COURSES;
INSERT INTO COURSES (CourseID, CourseName, CreditHours)
VALUES
    (101, 'Data Structures', 3),
    (102, 'Calculus', 4),
    (103, 'Database Systems', 3),
    (104, 'Linear Algebra', 3),
    (105, 'Introduction to Business', 3);

-- Populate Enrollments table
DELETE FROM ENROLLMENTS;
INSERT INTO ENROLLMENTS (EnrollmentID, StudentID, CourseID, Semester, Grade)
VALUES
    (1, 1, 101, 'Fall 2023', 'A'),
    (2, 2, 102, 'Fall 2023', 'B'),
    (3, 1, 103, 'Fall 2023', 'A'),
    (4, 3, 101, 'Fall 2023', 'A'),
    (5, 4, 104, 'Fall 2023', 'C'),
    (6, 5, 101, 'Fall 2023', 'A'),
    (7, 6, 102, 'Fall 2023', 'B');

-- Populate Courses_Instructors table
DELETE FROM COURSES_INSTRUCTORS;
INSERT INTO COURSES_INSTRUCTORS (CourseID, InstructorID)
VALUES
    (101, 2),
    (101, 3),
    (102, 3),
    (103, 2),
    (104, 3),
    (105, 4);