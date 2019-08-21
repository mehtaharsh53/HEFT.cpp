#include<iostream>
#include<bits/stdc++.h> 
#include<vector>
#include<algorithm>

using namespace std;

// ====================================================== Global declaration =================================================================== //
int N,Q;
int W[1000][50];								// Computation Cost  Matrix [N x Q]
int L[1000];									// Communication Startup Cost Matrix [1 x Q]
double avgW[1000];								// Average Computation Cost  Matrix [N x 1]
int B[50][50];									// Data Transfer Rate Matrix [Q x Q]
int data[1000][1000];							// Data need to tranfer between two tasks [N x N]
double RANK[1000];								// Upward Rank of Task [N x 1]
int EST[1000];									// Earliest execution start time [N x 1]
int EFT[1000];									// Earliest execution finish time [N x 1]
vector<pair<double, int>> rankTask;				// Pair of Rank and task	
vector<int> processorAvailableAt(1000,0);			// Processor available at time(or busy till time)

// =================================================== structer TaskProcessor ================================================================= //
//											1.On which processor task is executed																//
//												2.Actual Execution Start Time																	//
//												3.Actual Execution Finish Time																	//
// ============================================================================================================================================ //
struct TaskProcessor
{
    int processor;
    double AST;
    double AFT;
};
vector<struct TaskProcessor *> schedule;


// =============================================================== findRank =================================================================== //
// 										Following function find the upward rank of task i														//
// ============================================================================================================================================ //
double findRank(int i)
{
    double maximum = 0;
	double temp;
	for(int j = 0; j < N ; j++) 
	{
		// If j is a successor node. j is a child of i. 
	    if(data[i][j] != -1) 
		{
			temp = RANK[j];
		    if (maximum < data[i][j] + temp)
                maximum = data[i][j] + temp;
		}
    } 
    RANK[i] = avgW[i] + maximum;
	return RANK[i];
}


// ======================================================= isEntryTask ===================================================================== //
// 													Ckeck if it is an entry task															 //
// ========================================================================================================================================= //
int isEntryTask(int task)
{
    for(int i = 0 ; i < N ; i++)
        if( data[i][task] != -1 )
            return 0;
    return 1;
}

// ======================================================== calculateEST ==================================================================== //
int calculateEST(int task,int processor)
{
	int maximum = 0, sum = 0, tempAFT;
	int cmi;									// Communication Cost

	for(int i = 0 ; i < N ; i++)
	{
		if(data[i][task] != -1)
		{
			tempAFT = schedule[i]->AFT;
			if(schedule[i]->processor == processor)
				cmi = 0;					// cmi becomes 0, we assume that intraprocessor communication cost is negligible(From ResearchPaper)
			else
				cmi = L[schedule[i]->processor] + (data[i][task]/B[schedule[i]->processor][processor] );	// cmi Formula
			sum = tempAFT + cmi;						// AFT + cmi
		}
		if(sum > maximum)
		{
			maximum = sum;								// Max(predI(AFT+cmi))
		}
	}
	return max(processorAvailableAt[processor],maximum);		// max(availableprocessor[J],Max(predI(AFT+cmi)))
}


// ======================================================= HEFT ===================================================================== //
void HEFT()
{
    int task,processor;
	double minEFT = INT_MAX;
    
	for(int i = 0 ; i < N ; i++)
    {
        minEFT = INT_MAX;
        task = rankTask[i].second;
		// Check if it is an entry task
        if(isEntryTask(task))
        {
			EST[task] = 0;								// EST(Entry Task) = 0
            for(int j = 0 ; j < Q; j++)
            {
                if( minEFT > W[task][j])
                {
                    minEFT = W[task][j];
                    processor = j;
                }
            }
            schedule[task]->processor = processor;
            schedule[task]->AST = 0;
            schedule[task]->AFT = minEFT;
			processorAvailableAt[processor] = minEFT;
        }
        else
        {
			int currentEFT,currentEST;
			int saveEST;
			cout << "\n------------------------------------------------------------------------------\n";
            for(int j = 0 ; j < Q ; j++)
            {
				currentEST = calculateEST(task,j);
                currentEFT = W[task][j] + currentEST;
				cout << "Processor :" << j << ", EST :" << currentEST << ", EFT :" << currentEFT << endl;
				if(minEFT > currentEFT )
				{
					processor = j;
					minEFT = currentEFT;
					saveEST = currentEST;
				}
            }
			schedule[task]->processor = processor;
            schedule[task]->AST = saveEST;
            schedule[task]->AFT = minEFT;
			processorAvailableAt[processor] = minEFT;
        }
		cout << "\n==============================================================================\n";
		cout << "Task: " << task+1 << ", Processor: " << schedule[task]->processor+1;
        cout << ", AST: " << schedule[task]->AST << ", EFT: " << schedule[task]->AFT << endl;
	}
}

// ======================================================= Display ===================================================================== //
void displaySchedule()
{
	cout << "\n\n****************************** HEFT SCHEDULING *******************************\n\n";
	cout << "TASK\tPROCESSOR\tAST\tAFT" << endl;
	for(int i = 0 ; i < N ; i++)
		cout << i+1 << "\t" << schedule[i]->processor+1 << "\t\t" << schedule[i]->AST << "\t" << schedule[i]->AFT << endl; 
}

// =============================================================== main ===================================================================== //
int main()
{
	int sum ;
	// Read No of task and No of proc
	cin >> N >> Q ;

	// Initialize the Schedule, each processor to -1.
	for(int i=0; i<N; i++)
   	{
		struct TaskProcessor *temp = new TaskProcessor;
		temp->processor = -1;
		schedule.push_back(temp);
	} 
	
	// Read the computation costs of each task
    for(int i = 0 ; i < N ; i++)
	{
		sum = 0;
        for(int j = 0 ; j < Q ; j++)
		{
            cin >> W[i][j] ;
			sum += W[i][j];
		}
		// Finding the average computation cost.
		avgW[i] = (double)sum/Q;
		//cout << sum << endl;
	}

	// Read the matrix of data transfer rate between two processors
    for(int i = 0 ; i < Q ; i++)
        for(int j = 0 ; j < Q ; j++)
            cin >> B[i][j];	

	// Communication startup cost {	One dimensional matrix L(1xq)	}
	for(int i = 0 ; i < Q ; i++)
		cin >> L[i];

	// Read the matrix of data to be transferred between two tasks
    for(int i = 0 ; i < N ; i++)
        for(int j = 0 ; j < N ; j++)
            cin >> data[i][j];
	
	// Rank of exit task = avg computation cost.
	RANK[N - 1] = avgW[N - 1] ;
	rankTask.push_back( make_pair(RANK[N - 1],N - 1) );
	
	// Finding out the Rank of all the tasks
	for (int i = N - 2; i >= 0; i--) 
		rankTask.push_back( make_pair(findRank(i),i) );
	
	//sort on in Non increasing order.
	sort(rankTask.rbegin(),rankTask.rend());
	
	// Print Rank of each tasks. 
	for (int i = 0 ; i < N ; i++) 
		cout << rankTask[i].second + 1 << " : " << rankTask[i].first << endl;
	
	HEFT();

	displaySchedule();
}
