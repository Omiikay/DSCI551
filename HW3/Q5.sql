-- 1. Find full names of students advised by Dr. Smith
SELECT CONCAT(s.FirstName, ' ', s.LastName) AS FullName
FROM STUDENTS s
JOIN INSTRUCTORS i ON s.AdvisorID = i.InstructorID
WHERE i.InstructorName = 'Dr. Smith';

-- 2. Find names of advisors who advise only one student (without aggregate)
SELECT DISTINCT i1.InstructorName
FROM INSTRUCTORS i1
WHERE NOT EXISTS (
    SELECT 1
    FROM STUDENTS s1, STUDENTS s2
    WHERE s1.AdvisorID = i1.InstructorID 
    AND s2.AdvisorID = i1.InstructorID 
    AND s1.StudentID <> s2.StudentID
)
AND EXISTS (
    SELECT 1
    FROM STUDENTS s
    WHERE s.AdvisorID = i1.InstructorID
);

-- 3. Find names of advisors who advise only one student (with aggregate)
SELECT i.InstructorName
FROM INSTRUCTORS i
JOIN STUDENTS s ON i.InstructorID = s.AdvisorID
GROUP BY i.InstructorID, i.InstructorName
HAVING COUNT(*) = 1;

-- 4. Find full names of students who enroll in at least two different courses (without aggregate)
SELECT DISTINCT CONCAT(s.FirstName, ' ', s.LastName) AS FullName
FROM STUDENTS s
JOIN ENROLLMENTS e1 ON s.StudentID = e1.StudentID
JOIN ENROLLMENTS e2 ON s.StudentID = e2.StudentID
WHERE e1.CourseID != e2.CourseID;

-- 5. Find names of students who enroll in at least two different courses (with aggregate)
SELECT CONCAT(s.FirstName, ' ', s.LastName) AS FullName
FROM STUDENTS s
JOIN ENROLLMENTS e ON s.StudentID = e.StudentID
GROUP BY s.StudentID, s.FirstName, s.LastName
HAVING COUNT(DISTINCT e.CourseID) >= 2;

-- 6. Find names of the most popular courses
WITH CourseEnrollments AS (
    SELECT c.CourseName, COUNT(e.StudentID) as EnrollmentCount
    FROM COURSES c
    JOIN ENROLLMENTS e ON c.CourseID = e.CourseID
    GROUP BY c.CourseID, c.CourseName
)
SELECT CourseName
FROM CourseEnrollments
WHERE EnrollmentCount = (
    SELECT MAX(EnrollmentCount)
    FROM CourseEnrollments
);

-- 7. For each student by id, find out their total credit hours
SELECT 
    s.StudentID,
    COALESCE(SUM(c.CreditHours), 0) as TotalCreditHours
FROM STUDENTS s
LEFT JOIN ENROLLMENTS e ON s.StudentID = e.StudentID
LEFT JOIN COURSES c ON e.CourseID = c.CourseID
GROUP BY s.StudentID;

-- 8. Using outer join, find ids of students who did not take any courses
SELECT s.StudentID
FROM STUDENTS s
LEFT JOIN ENROLLMENTS e ON s.StudentID = e.StudentID
WHERE e.EnrollmentID IS NULL;

-- 9. Using subquery, find ids of students who did not take any courses
SELECT StudentID
FROM STUDENTS
WHERE StudentID NOT IN (
    SELECT DISTINCT StudentID
    FROM ENROLLMENTS
);

-- 10. Find names of students who took both courses 101 and 103
SELECT CONCAT(s.FirstName, ' ', s.LastName) AS FullName
FROM STUDENTS s
WHERE EXISTS (
    SELECT 1 
    FROM ENROLLMENTS e1 
    WHERE e1.StudentID = s.StudentID 
    AND e1.CourseID = 101
)
AND EXISTS (
    SELECT 1 
    FROM ENROLLMENTS e2 
    WHERE e2.StudentID = s.StudentID 
    AND e2.CourseID = 103
);