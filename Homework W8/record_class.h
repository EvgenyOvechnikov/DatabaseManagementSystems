// Evgeny Ovechnikov
// Course: CS540 - Database Management Systems
// Homework: Join Algorithms (w8)

/* This is a skeleton code for Optimized Merge Sort, you can make modifications as long as you meet
   all question requirements*/  
/* This record_class.h contains the class Records, which can be used to store tuples form Emp.csv (stored
in the EmpRecords) and Dept.csv (Stored in DeptRecords) files.
*/

#include <cstring>
#include <sstream>
#include <fstream>

#define NAME_LENGTH 40

using namespace std;

class Records{
    public:
    
    struct EmpRecord {
        int eid;
        //string ename;
        char ename[NAME_LENGTH];
        int age;
        double salary;
    }emp_record;

    struct DeptRecord {
        int did;
        //string dname;
        char dname[NAME_LENGTH];
        double budget;
        int managerid;
    }dept_record;

    /*** You can add more variables if you want below ***/

    int no_values = 0; //You can use this to check if you've don't have any more tuples
    int number_of_emp_records = 0; // Tracks number of emp_records you have on the buffer
    int number_of_dept_records = 0; //Track number of dept_records you have on the buffer
};

// Grab a single block from the Emp.csv file and put it inside the EmpRecord structure of the Records Class
Records Grab_Emp_Record(fstream &empin) {
    string line, word;
    Records emp;
    // grab entire line
    if (getline(empin, line, '\n')) {
        // turn line into a stream
        stringstream s(line);
        // gets everything in stream up to comma
        getline(s, word,',');
        emp.emp_record.eid = stoi(word);
        getline(s, word, ',');
        //emp.emp_record.ename = word;
        strcpy(emp.emp_record.ename, word.c_str());
        getline(s, word, ',');
        emp.emp_record.age = stoi(word);
        getline(s, word, ',');
        emp.emp_record.salary = stod(word);

        //Ensuring that you cannot use both structure (EmpEecord, DeptRecord) at the same memory block / time 
        emp.dept_record.did = 0;
        //emp.dept_record.dname = "";
        memset(emp.dept_record.dname, 0, NAME_LENGTH);
        emp.dept_record.budget = 0;
        emp.dept_record.managerid = 0;

        return emp;
    } else {
        emp.no_values = -1;
        return emp;
    }
}

// Grab a single block from the Dept.csv file and put it inside the DeptRecord structure of the Records Class
Records Grab_Dept_Record(fstream &deptin) {
    string line, word;
    //DeptRecord dept;
    Records dept;
    if (getline(deptin, line, '\n')) {
        stringstream s(line);
        getline(s, word,',');
        dept.dept_record.did = stoi(word);
        getline(s, word, ',');
        //dept.dept_record.dname = word;
        strcpy(dept.dept_record.dname, word.c_str());
        getline(s, word, ',');
        dept.dept_record.budget = stod(word);
        getline(s, word, ',');
        dept.dept_record.managerid = stoi(word);

        //Ensuring that you cannot use both structure (EmpEecord, DeptRecord) at the same memory block / time 
        dept.emp_record.eid = 0;
        //dept.emp_record.ename = "";
        memset(dept.emp_record.ename, 0, NAME_LENGTH);
        dept.emp_record.age = 0;
        dept.emp_record.salary = 0;

        return dept;
    } else {
        dept.no_values = -1;
        return dept;
    }
}