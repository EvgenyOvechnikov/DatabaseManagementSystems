// Evgeny Ovechnikov
// Course: CS540 - Database Management Systems
// Homework: Join Algorithms (w8)

/* This is a skeleton code for Optimized Merge Sort, you can make modifications as long as you meet
   all question requirements*/  

#include "record_class.h"
#include <math.h>

using namespace std;

//defines how many blocks are available in the Main Memory 
#define buffer_size 11  // I use two buffers, one for Emp and one for Dept, each 11 records
#define INT_MAX 2147483647
const string passEmp_filename = "passEmp";
const string passDept_filename = "passDept";
const string passJoin_filename = "passJoin";

//use this class object of size 22 as your main memory
// After considerations, I decided to use separate buffers for Emp and for Dept.
// This simplifies math when we have to do >1 merging pass due to buffer size limit.
Records buffEmp[buffer_size]; // each buffer is 11 records, total of 22, meeting the requirement
Records buffDept[buffer_size]; 
int sEmp = 0;
int sDept = 0;
int counters[buffer_size];  // Global array for (hopefully) saving memory.

// Defining comparator for Emp (by eid)
int compareEid(const void* a, const void* b)
{
    const Records* x = (Records*)a;
    const Records* y = (Records*)b;

    if (x->emp_record.eid > y->emp_record.eid) return 1;
    if (x->emp_record.eid < y->emp_record.eid) return -1;

    return 0;
}

// Defining comparator for Dept (by managerid)
int compareManagerid(const void* a, const void* b)
{
    const Records* x = (Records*)a;
    const Records* y = (Records*)b;

    if (x->dept_record.managerid > y->dept_record.managerid) return 1;
    if (x->dept_record.managerid < y->dept_record.managerid) return -1;

    return 0;
}

void savePass0(Records* buf, unsigned int page, bool saveEmp) {
    fstream pass0;
    pass0.open(saveEmp ? passEmp_filename + "0" : passDept_filename + "0", ios::binary | ios::in | ios::out);
    pass0.seekp(page * sizeof(buffEmp), ios_base::beg);

    int nRecords = buf->number_of_emp_records;
    char* t = new char[sizeof(Records)];
    for (int i = 0; i < nRecords; i++) {
        *reinterpret_cast<Records*>(t) = buffEmp[i];
        pass0.write(t, sizeof(Records));
    }
    pass0.close();
}

// This method doesn't do entire merge. It only does first merge passes when it's needed.
int MergeFirstPasses(int order, bool passEmp) {
    int buffPower = pow(buffer_size, order);
    Records* buffer = passEmp ? &buffEmp[0] : &buffDept[0];
    fstream passIn, passOut;

    passIn.open(passEmp ? passEmp_filename + to_string(order - 1) : passDept_filename + to_string(order - 1), ios::binary | ios::in);
    char* tRead = new char[sizeof(Records)];
    char* tWrite = new char[sizeof(Records)];

    passOut.open(passEmp ? passEmp_filename + to_string(order) : passDept_filename + to_string(order), ios::binary | ios::out);

    // The logic is the same for Emps and Depts. Just checking where to get/put the data.
    bool eof = false;
    int seg = 0;
    for (; !eof; seg++) {
        int s = 0;
        // Filling up the buffer
        for (; s <= buffer_size; s++) {
            passIn.seekp((seg * buffer_size + s) * buffPower * sizeof(Records), ios::ios_base::beg);
            passIn.read(tRead, sizeof(Records));
            if (passIn.eof()) {
                // We reached the end of file. Current s data is garbage. Exiting.
                passIn.close();
                passIn.open(passEmp ? passEmp_filename + to_string(order - 1) : passDept_filename + to_string(order - 1), ios::binary | ios::in);
                eof = true;
                break;
            }

            if (s < buffer_size) {
                buffer[s] = *reinterpret_cast<Records*>(tRead);
                counters[s] = 0;
            }
        }

        if (passEmp) {
            sEmp = s;
        }
        else {
            sDept = s;
        }

        // if there's no need to merge, closing streams, deleting output file and exiting right here
        if (seg == 0 && eof) {
            passIn.close();
            passOut.close();
            remove(passEmp ? (passEmp_filename + to_string(order)).c_str() : (passDept_filename + to_string(order)).c_str());
            return order - 1;
        }

        // Merging
        while (true)
        {
            int minI = -1;
            int minId = INT_MAX;
            // finding next record to insert in the output stream
            for (int i = 0; i < s; i++) {
                if ((passEmp && buffer[i].emp_record.eid < minId && counters[i] < buffPower) ||
                    !passEmp && buffer[i].dept_record.managerid < minId && counters[i] < buffPower) {
                    minI = i;
                    minId = passEmp ? buffer[i].emp_record.eid : buffer[i].dept_record.managerid;
                }
            }

            if (minI == -1) {
                // Done with current run
                break;
            }

            // inserting record, updating counter, reading next
            *reinterpret_cast<Records*>(tWrite) = buffer[minI];
            passOut.write(tWrite, sizeof(Records));
            counters[minI]++;

            passIn.seekp(((seg * buffer_size + minI) * buffPower + counters[minI]) * sizeof(Records), ios::ios_base::beg);
            passIn.read(tRead, sizeof(Records));

            // Checking if the end of file
            if (passIn.eof()) {
                passIn.close();
                passIn.open(passEmp ? passEmp_filename + to_string(order - 1) : passDept_filename + to_string(order - 1), ios::binary | ios::in);
                counters[minI] = buffPower;
            }
            else {
                // Reading new record into the buffer
                buffer[minI] = *reinterpret_cast<Records*>(tRead);
            }
        }
    }

    passIn.close();
    passOut.close();

    return seg == 1 ? order : MergeFirstPasses(order + 1, passEmp);
}

/***You can change return type and arguments as you want.***/

//Sorting the buffers in main_memory and storing the sorted records into a temporary file (runs) 
void Sort_Buffer(Records* buf, bool sortEmp){
    //Remember: You can use only [AT MOST] 22 blocks for sorting the records / tuples and create the runs
    if (sortEmp) {
        // Sorting Emp by eid
        int numr = buf->number_of_emp_records;
        std::qsort(buf, buf->number_of_emp_records, sizeof(Records), compareEid);
        buf->number_of_emp_records = numr;
    }
    else {
        // Sorting Dept by managerid
        int numr = buf->number_of_emp_records;
        std::qsort(buf, buf->number_of_emp_records, sizeof(Records), compareManagerid);
        buf->number_of_emp_records = numr;
    }
    return;
}

//Prints out the attributes from empRecord and deptRecord when a join condition is met 
//and puts it in file Join.csv
void PrintJoin(fstream& sortOut) {
    fstream passN;
    passN.open(passJoin_filename, ios::binary | ios::in);

    while (true)
    {
        char* t = new char[sizeof(Records)];
        passN.read(t, sizeof(Records));
        Records r = *reinterpret_cast<Records*>(t);

        // C++ EOF shenanigans.
        // Loop break has to be here to truncate the last portion of data (garbage).
        if (passN.eof()) break;

        sortOut << r.emp_record.eid << ","
            << r.emp_record.ename << ","
            << r.emp_record.age << ","
            << (long long)r.emp_record.salary << ","
            << r.dept_record.did << ","
            << r.dept_record.dname << ","
            << (long long)r.dept_record.budget << ","
            << r.dept_record.managerid << endl;
    }

    passN.close();
    return;
}

//Use main memory to Merge and Join tuples 
//which are already sorted in 'runs' of the relations Dept and Emp 
void Merge_Join_Runs(int orderEmp, int orderDept){
    //and store the Joined new tuples using PrintJoin() 

    // Some stuff we need
    int* countersEmp = new int[sEmp];
    int* countersDept = new int[sDept];
    //memset((void*)countersEmp, 0, sEmp);  // doesn't seem to work
    //memset((void*)countersDept, 0, sDept);
    for (int i = 0; i < sEmp; i++) {
        countersEmp[i] = 0;
    }
    for (int i = 0; i < sDept; i++) {
        countersDept[i] = 0;
    }
    char* tRead = new char[sizeof(Records)];
    char* tWrite = new char[sizeof(Records)];

    int powerEmp = pow(buffer_size, orderEmp + 1);
    int powerDept = pow(buffer_size, orderDept + 1);

    fstream passEmp, passDept, passJoin;
    passEmp.open(passEmp_filename + to_string(orderEmp), ios::binary | ios::in);
    passDept.open(passDept_filename + to_string(orderDept), ios::binary | ios::in);
    passJoin.open(passJoin_filename, ios::binary | ios::out);

    // starting joining
    while (true) {
        // Picking the first Employee
        int minI = -1;
        int minId = INT_MAX;
        for (int i = 0; i < sEmp; i++) {
            if (buffEmp[i].emp_record.eid < minId && countersEmp[i] < powerEmp) {
                minId = buffEmp[i].emp_record.eid;
                minI = i;
            }
        }

        int minJ = -1;
        int minManagerId = INT_MAX;
        for (int j = 0; j < sDept; j++) {
            if (buffDept[j].dept_record.managerid < minManagerId && countersDept[j] < powerDept) {
                minManagerId = buffDept[j].dept_record.managerid;
                minJ = j;
            }
        }

        if (minI == -1 || minJ == -1) {
            // We're done.
            break;
        }

        // Comparing the join conditions
        if (buffEmp[minI].emp_record.eid > buffDept[minJ].dept_record.managerid) {
            // Reading next Department
            countersDept[minJ]++;
            passDept.seekp((minJ * powerDept + countersDept[minJ]) * sizeof(Records), ios::ios_base::beg);
            passDept.read(tRead, sizeof(Records));

            // Checking if the end of file
            if (passDept.eof()) {
                passDept.close();
                passDept.open(passDept_filename + to_string(orderDept), ios::binary | ios::in);
                countersDept[minJ] = powerDept;
            }
            else {
                // Reading new record into the buffer
                buffDept[minJ] = *reinterpret_cast<Records*>(tRead);
            }
        }
        else if (buffEmp[minI].emp_record.eid < buffDept[minJ].dept_record.managerid) {
            // Reading next Employee
            countersEmp[minI]++;
            passEmp.seekp((minI * powerEmp + countersEmp[minI]) * sizeof(Records), ios::ios_base::beg);
            passEmp.read(tRead, sizeof(Records));

            // Checking if the end of file
            if (passEmp.eof()) {
                passEmp.close();
                passEmp.open(passEmp_filename + to_string(orderEmp), ios::binary | ios::in);
                countersEmp[minI] = powerEmp;
            }
            else {
                // Reading new record into the buffer
                buffEmp[minI] = *reinterpret_cast<Records*>(tRead);
            }

        }
        else {
            // Finally, creating join record and writing it to the Join file
            buffEmp[minI].dept_record = buffDept[minJ].dept_record;
            // inserting record, updating counter
            *reinterpret_cast<Records*>(tWrite) = buffEmp[minI];
            passJoin.write(tWrite, sizeof(Records));
            // We advancing only Department records, since we want to catch the many possible matches
            // A lot of code is the same as first condition, however, I'll leave it here
            // in case we need to handle many-to-many join
            countersDept[minJ]++;
            passDept.seekp((minJ * powerDept + countersDept[minJ]) * sizeof(Records), ios::ios_base::beg);
            passDept.read(tRead, sizeof(Records));

            // Checking if the end of file
            if (passDept.eof()) {
                passDept.close();
                passDept.open(passDept_filename + to_string(orderDept), ios::binary | ios::in);
                countersDept[minJ] = powerDept;
            }
            else {
                // Reading new record into the buffer
                buffDept[minJ] = *reinterpret_cast<Records*>(tRead);
            }
        }
    }

    passEmp.close();
    passDept.close();
    passJoin.close();

    return;
}

int main() {

    //Open file streams to read and write
    //Opening out two relations Emp.csv and Dept.csv which we want to join
    fstream empin;
    fstream deptin;
    empin.open("Emp.csv", ios::in);
    deptin.open("Dept.csv", ios::in);
   
    //Creating the Join.csv file where we will store our joined results
    remove("Join.csv");
    fstream joinout;
    joinout.open("Join.csv", ios::out | ios::app);

    // Creating 0-length files for pass 0
    remove((passEmp_filename + "0").c_str());
    remove((passDept_filename + "0").c_str());
    fstream pass0;
    pass0.open((passEmp_filename + "0"), ios::binary | ios::out);
    pass0.close();
    pass0.open((passDept_filename + "0"), ios::binary | ios::out);
    pass0.close();

    //1. Create runs for Dept and Emp which are sorted using Sort_Buffer()
    // Two runs, one for Emp, one for Dept
    bool processingEmp = false;
    for (int dummy = 0; dummy < 2; dummy++) {
        processingEmp = !processingEmp;
        unsigned int pagesCount = 0;
        bool stoppingToken = false;
        do
        {
            // Handling some edge cases, when number of records is exact multiple of buffer capacity...
            for (int i = 0; i < buffer_size; i++) {
                Records r = processingEmp ? Grab_Emp_Record(empin) : Grab_Dept_Record(deptin);
                if (r.no_values) {
                    stoppingToken = true;
                    buffEmp[0].number_of_emp_records = i;
                    break;
                }
                else {
                    buffEmp[i] = r;
                    buffEmp[0].number_of_emp_records = i + 1;
                }
            }
            if (buffEmp[0].number_of_emp_records) {
                Sort_Buffer(buffEmp, processingEmp);
                savePass0(buffEmp, pagesCount, processingEmp);
                pagesCount++;
            }
        } while (!stoppingToken);
    }

    //2. Use Merge_Join_Runs() to Join the runs of Dept and Emp relations 
    // In case of a large data, we possibly need to do some first passes,
    // occationally establishing the buffer and a pass number.
    int resEmp = MergeFirstPasses(1, true);
    int resDept = MergeFirstPasses(1, false);
    Merge_Join_Runs(resEmp, resDept);

    PrintJoin(joinout);

    //Please delete the temporary files (runs) after you've joined both Emp.csv and Dept.csv
    for (int j = 0; j <= resEmp; j++) {
        remove((passEmp_filename + to_string(j)).c_str());
    }
    for (int j = 0; j <= resDept; j++) {
        remove((passDept_filename + to_string(j)).c_str());
    }
    remove(passJoin_filename.c_str());

    empin.close();
    deptin.close();
    joinout.close();
    return 0;
}
