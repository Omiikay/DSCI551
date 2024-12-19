-- Create a new database
CREATE DATABASE IF NOT EXISTS dsci551;

-- Use DataBase 
USE dsci551;

-- Drop tables
DROP TABLE IF EXISTS STUDENTS;
DROP TABLE IF EXISTS COURSES;
DROP TABLE IF EXISTS INSTRUCTORS;
DROP TABLE IF EXISTS ENROLLMENTS;
DROP TABLE IF EXISTS COURSES_INSTRUCTORS;

-- Creare tables
-- Table to store instructors/advisors info, no redundancies here.
CREATE TABLE IF NOT EXISTS INSTRUCTORS (
    InstructorID INT(10) PRIMARY KEY,
    InstructorName VARCHAR(50) NOT NULL
);

-- Table to store students info, redundancies is removed as only AdvisorID stored here,
-- and the detail of each Advisor is stored in INSTRUCTORS table.
CREATE TABLE IF NOT EXISTS STUDENTS (
    StudentID INT(10) PRIMARY KEY,
    AdvisorID INT(10) NOT NULL,
    FirstName VARCHAR(50) NOT NULL,
    LastName VARCHAR(50) NOT NULL,
    Major VARCHAR(50) NOT NULL,
    Email VARCHAR(50),
    FOREIGN KEY (AdvisorID) REFERENCES INSTRUCTORS(InstructorID)
);

-- Table to store students info, redundancies is removed as only InstructorID stored here,
-- and the the instructor attribute is removed here to avoid redundancy.
CREATE TABLE IF NOT EXISTS COURSES (
    CourseID INT(10) PRIMARY KEY,
    CourseName VARCHAR(30) NOT NULL,
    CreditHours INT(10)
);

-- Table to store enrollment info, no obvious redundancy exists.
Create TABLE IF NOT EXISTS ENROLLMENTS (
    EnrollmentID INT(10) PRIMARY KEY,
    CourseID INT(10) NOT NULL,
    StudentID INT(10) NOT NULL,
    Semester VARCHAR(50) NOT NULL,
    Grade VARCHAR(2),
    FOREIGN KEY (CourseID) REFERENCES COURSES(CourseID),
    FOREIGN KEY (StudentID) REFERENCES STUDENTS(StudentID)
);

-- Table to store the many-to-many relationship between courses and instructors.
CREATE TABLE IF NOT EXISTS COURSES_INSTRUCTORS (
    CourseID INT(10) NOT NULL,
    InstructorID INT(10) NOT NULL,
    PRIMARY KEY (CourseID, InstructorID),
    FOREIGN KEY (CourseID) REFERENCES COURSES(CourseID),
    FOREIGN KEY (InstructorID) REFERENCES INSTRUCTORS(InstructorID)
);