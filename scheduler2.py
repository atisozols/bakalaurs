from collections import defaultdict
import sqlite3 as sqlite
import prettytable as prettytable
import random as rnd
from enum import Enum

POPULATION_SIZE = 100
NUMB_OF_ELITE_SCHEDULES = 5
TOURNAMENT_SELECTION_SIZE = 30
MUTATION_RATE = 0.005
VERBOSE_FLAG = True

class DatabaseManager:
    def __init__(self):
        self._conn = sqlite.connect('class_schedule.db')
        self._c = self._conn.cursor()
        self._meetingTimes = self._select_meeting_times()
        self._instructors = self._select_instructors()
        self._courses = self._select_courses()
        self._depts = self._select_depts()
        self._numberOfClasses = 0
        for i in range(0, len(self._depts)):
            self._numberOfClasses += len(self._depts[i].get_courses())
    def _select_meeting_times(self):
        self._c.execute("SELECT * FROM meeting_time")
        meetingTimes = self._c.fetchall()
        returnMeetingTimes = []
        for i in range(0, len(meetingTimes)):
            returnMeetingTimes.append(MeetingTime(meetingTimes[i][0], meetingTimes[i][1]))
        return returnMeetingTimes
    def _select_instructors(self):
        self._c.execute("SELECT * FROM instructor")
        instructors = self._c.fetchall()
        returnInstructors = []
        for i in range(0, len(instructors)):
            returnInstructors.append(Instructor(instructors[i][0], instructors[i][1], self._select_instructor_availability(instructors[i][0])))
        return returnInstructors
    def _select_instructor_availability(self, instructor):
        self._c.execute("SELECT * from instructor_availability where instructor_id = '" + instructor + "'")
        instructorMTsRS = self._c.fetchall()
        instructorMTs = []
        for i in range(0, len(instructorMTsRS)): instructorMTs.append(instructorMTsRS[i][1])
        instructorAvailability = list()
        for i in range(0, len(self._meetingTimes)):
            if self._meetingTimes[i].get_id() in instructorMTs:
                instructorAvailability.append(self._meetingTimes[i])
        return instructorAvailability
    def _select_courses(self):
        self._c.execute("SELECT * FROM course")
        courses = self._c.fetchall()
        returnCourses = []
        for i in range(0, len(courses)):
            returnCourses.append(
                Course(courses[i][0], courses[i][1], self._select_course_instructors(courses[i][0]), courses[i][2]))
        return returnCourses
    def _select_depts(self):
        self._c.execute("SELECT * FROM dept")
        depts = self._c.fetchall()
        returnDepts = []
        for i in range(0, len(depts)):
            returnDepts.append(Department(depts[i][0], self._select_dept_courses(depts[i][0])))
        return returnDepts
    def _select_course_instructors(self, courseNumber):
        self._c.execute("SELECT * FROM course_instructor where course_number == '" + courseNumber + "'")
        dbInstructorNumbers = self._c.fetchall()
        instructorNumbers = []
        for i in range(0, len(dbInstructorNumbers)):
            instructorNumbers.append(dbInstructorNumbers[i][1])
        returnValue = []
        for i in range(0, len(self._instructors)):
           if  self._instructors[i].get_id() in instructorNumbers:
               returnValue.append(self._instructors[i])
        return returnValue
    def _select_dept_courses(self, deptName):
        self._c.execute("SELECT * FROM dept_course where name == '" + deptName + "'")
        dbCourseNumbers = self._c.fetchall()
        courseNumbers = []
        for i in range(0, len(dbCourseNumbers)):
            courseNumbers.append(dbCourseNumbers[i][1])
        returnValue = []
        for i in range(0, len(self._courses)):
           if self._courses[i].get_number() in courseNumbers:
               returnValue.append(self._courses[i])
        return returnValue
    
    def get_instructors(self): return self._instructors
    def get_courses(self): return self._courses
    def get_depts(self): return self._depts
    def get_meetingTimes(self): return self._meetingTimes
    def get_numberOfClasses(self): return self._numberOfClasses

class Schedule:
    def __init__(self):
        self._data = dbMgr
        self._classes = []
        self._conflicts = []
        self._fitness = -1
        self._classNumb = 0
        self._isFitnessChanged = True
        self.meeting_map = defaultdict(list)
    def get_classes(self):
        self._isFitnessChanged = True
        return self._classes
    def set_class(self, id, other_schedule):
        # remove meeting time of old class, add meeting time of new class
        old_class = self._classes[id]
        new_class = other_schedule.get_classes()[id]

        old_id = old_class.get_meetingTime().get_id()
        new_id = new_class.get_meetingTime().get_id()

        self.meeting_map[old_id].remove(old_class)
        self.meeting_map[new_id].append(new_class)

        self._classes[id] = new_class

    def get_conflicts(self): return self._conflicts
    def get_courses_by_dept_and_meetingTime(self):
        courses_by_dept_and_meetingTime = defaultdict(lambda: defaultdict(list))
        for class_obj in self._classes:
            dept_name = class_obj.get_dept().get_name()
            meetingTime_id = class_obj.get_meetingTime().get_id()
            courses_by_dept_and_meetingTime[dept_name][meetingTime_id].append([class_obj.get_course(), class_obj.get_instructor()])
        return courses_by_dept_and_meetingTime
    def get_fitness(self):
        if (self._isFitnessChanged == True):
            self._fitness = self.calculate_fitness()
            self._isFitnessChanged = False
        return self._fitness
    def initialize(self):
        depts = self._data.get_depts()
        for i in range(0, len(depts)):
            courses = depts[i].get_courses()
            for j in range(0, len(courses)):
                newClass = Class(self._classNumb, depts[i], courses[j])
                self._classNumb += 1
                meeting_time = dbMgr.get_meetingTimes()[rnd.randrange(0, len(dbMgr.get_meetingTimes()))]
                newClass.set_meetingTime(meeting_time)
                newClass.set_instructor(courses[j].get_instructors()[rnd.randrange(0, len(courses[j].get_instructors()))])
                self._classes.append(newClass)
                self.meeting_map[meeting_time.get_id()].append(newClass)
        return self

    def calculate_fitness(self):
        self._conflicts = []
        for id, classes in self.meeting_map.items():
            if len(classes) < 2:
                if len(classes) == 1 and classes[0].get_meetingTime() not in classes[0].get_instructor().get_availability():
                    conflictBetweenClasses = list()
                    conflictBetweenClasses.append(classes[0])
                    self._conflicts.append(Conflict(Conflict.ConflictType.INSTRUCTOR_AVAILABILITY, conflictBetweenClasses))
                continue

            instructors = dict()
            departments = dict()

            # if an instructor already has a booked class for that meeting time, he is not available
            # if a department already is booked for that meeting time, it cannot be used
            for i in range(0, len(classes)):
                if classes[i].get_meetingTime() not in classes[i].get_instructor().get_availability():
                    conflictBetweenClasses = list()
                    conflictBetweenClasses.append(classes[0])
                    self._conflicts.append(Conflict(Conflict.ConflictType.INSTRUCTOR_AVAILABILITY, conflictBetweenClasses))

                instructor_id = classes[i].get_instructor().get_id()
                if (instructors.get(instructor_id) == None):
                    instructors[instructor_id] = i
                else:
                    instructorBookingConflict = list()
                    instructorBookingConflict.append(classes[i])
                    instructorBookingConflict.append(classes[instructors.get(instructor_id)])
                    self._conflicts.append(Conflict(Conflict.ConflictType.INSTRUCTOR_BOOKING, instructorBookingConflict))

                department_id = classes[i].get_dept().get_name()
                if (departments.get(department_id) == None):
                    departments[department_id] = i
                else:
                    classBookingConflict = list()
                    classBookingConflict.append(classes[i])
                    classBookingConflict.append(classes[departments.get(department_id)])
                    self._conflicts.append(Conflict(Conflict.ConflictType.CLASS_BOOKING, classBookingConflict))

        return 1/(len(self._conflicts)+1)
    
    def gapFitness(self):
        d = self.get_courses_by_dept_and_meetingTime()
        fitness = 0
        for clas in d:
            gapsInEachDay = [0, 0, 0, 0, 0]
            lessonsInEachDay = [0, 0, 0, 0, 0]
            for day in range(1, 6):
                gaps = 0
                is_gap = False

                for lesson in range(1, 9):
                    id = str(day) + "." + str(lesson)
                    if len(d[clas][id]) == 0 and not is_gap:
                        is_gap = True
                    elif len(d[clas][id]) == 0 and is_gap:
                        gaps += 1
                    elif len(d[clas][id]) != 0:
                        if is_gap:
                            gaps += 1
                            is_gap = False
                        lessonsInEachDay[day - 1] += 1

                gapsInEachDay[day - 1] = gaps
          
            for gap in gapsInEachDay:
                if gap > 0:
                    fitness += 1  

            if max(lessonsInEachDay) - min(lessonsInEachDay) > 2:
                fitness += (max(lessonsInEachDay) - min(lessonsInEachDay))
            
        return 1/(fitness + 1)

    def __str__(self):
        returnValue = ""
        for i in range(0, len(self._classes)-1):
            returnValue += str(self._classes[i]) + ", "
        returnValue += str(self._classes[len(self._classes)-1])
        return returnValue

class Population:
    def __init__(self, size):
        self._size = size
        self._data = dbMgr
        self._schedules = []
        for i in range(0, size): self._schedules.append(Schedule().initialize())
    def get_schedules(self): return self._schedules

class GeneticAlgorithm:
    def evolve(self, population): return self._mutate_population(self._crossover_population(population))
    def _crossover_population(self, pop):
        crossover_pop = Population(0)
        for i in range(NUMB_OF_ELITE_SCHEDULES):
            crossover_pop.get_schedules().append(pop.get_schedules()[i])
        i = NUMB_OF_ELITE_SCHEDULES
        while i < POPULATION_SIZE:
            schedule1 = self._select_tournament_population(pop).get_schedules()[0]
            schedule2 = self._select_tournament_population(pop).get_schedules()[0]
            crossover_pop.get_schedules().append(self._crossover_schedule(schedule1, schedule2))
            i += 1
        return crossover_pop
    def _mutate_population(self, population):
        for i in range(NUMB_OF_ELITE_SCHEDULES, POPULATION_SIZE):
            self._mutate_schedule(population.get_schedules()[i])
        return population
    def _crossover_schedule(self, schedule1, schedule2):
        crossoverSchedule = Schedule().initialize()
        for i in range(0, len(crossoverSchedule.get_classes())):
            if (rnd.random() > 0.5): crossoverSchedule.set_class(i, schedule1)
            else: crossoverSchedule.set_class(i, schedule2)
        return crossoverSchedule
    def _mutate_schedule(self, mutateSchedule):
        schedule = Schedule().initialize()
        for i in range(0, len(mutateSchedule.get_classes())):
            if(MUTATION_RATE > rnd.random()): mutateSchedule.set_class(i, schedule)
        return mutateSchedule
    def _select_tournament_population(self, pop):
        tournament_pop = Population(0)
        i = 0
        while i < TOURNAMENT_SELECTION_SIZE:
            tournament_pop.get_schedules().append(pop.get_schedules()[rnd.randrange(0, POPULATION_SIZE)])
            i += 1
        tournament_pop.get_schedules().sort(key=lambda x: x.get_fitness(), reverse=True)
        return tournament_pop

class Course:
    def __init__(self, number, name, instructors, maxNumbOfStudents):
        self._number = number
        self._name = name
        self._maxNumbOfStudents = maxNumbOfStudents
        self._instructors = instructors
    def get_number(self): return self._number
    def get_name(self): return self._name
    def get_instructors(self): return self._instructors
    def get_maxNumbOfStudents(self): return self._maxNumbOfStudents
    def __str__(self): return self._name

class Instructor:
    def __init__(self, id, name, availability):
        self._id = id
        self._name = name
        self._availability = availability
    def get_id(self): return self._id
    def get_name(self): return self._name
    def get_availability(self): return self._availability
    def __str__(self): return self._name

class MeetingTime:
    def __init__(self, id, time):
        self._id = id
        self._time = time
    def get_id(self): return self._id
    def get_time(self): return self._time

class Department:
    def __init__(self, name, courses):
        self._name = name
        self._courses = courses
    def get_name(self): return self._name
    def get_courses(self): return self._courses

class Class:
    def __init__(self, id, dept, course):
        self._id = id
        self._dept = dept
        self._course = course
        self._instructor = None
        self._meetingTime = None
    def get_id(self): return self._id
    def get_dept(self): return self._dept
    def get_course(self): return self._course
    def get_instructor(self): return self._instructor
    def get_meetingTime(self): return self._meetingTime
    def set_instructor(self, instructor): self._instructor = instructor
    def set_meetingTime(self, meetingTime): self._meetingTime = meetingTime
    def __str__(self):
        return str(self._dept.get_name()) + "," + str(self._course.get_number()) +  "," + str(self._instructor.get_id()) + "," + str(self._meetingTime.get_id())
    
class Conflict:
    class ConflictType(Enum):
        INSTRUCTOR_BOOKING = 1
        CLASS_BOOKING = 2
        NUMB_OF_STUDENTS = 3
        INSTRUCTOR_AVAILABILITY = 4
    def __init__(self, conflictType, conflictBetweenClasses):
        self._conflictType = conflictType
        self._conflictBetweenClasses = conflictBetweenClasses
    def get_conflictType(self): return self._conflictType
    def get_conflictBetweenClasses(self): return self._conflictBetweenClasses
    def __str__(self): return str(self._conflictType)+" "+str(" and ".join(map(str, self._conflictBetweenClasses)))

class DisplayMgr:  
    @staticmethod
    def display_schedule_by_dept_and_meetingTime_console(schedule):
        courses_by_dept_and_meetingTime = schedule.get_courses_by_dept_and_meetingTime()
        meeting_times = sorted(schedule._data.get_meetingTimes(), key=lambda mt: mt.get_id())

        table = prettytable.PrettyTable(['Meeting Time'] + [dept_name for dept_name in courses_by_dept_and_meetingTime.keys()])

        for meeting_time in meeting_times:
            row = [meeting_time.get_time()]
            for dept_name in courses_by_dept_and_meetingTime.keys():
                courses = courses_by_dept_and_meetingTime[dept_name][meeting_time.get_id()]
                course_names = ", ".join([f"{course[0].get_name()} ({course[1].get_id3()})" for course in courses])
                row.append(course_names)
            table.add_row(row)

        print(table)
    

    def display_schedule_by_dept_and_meetingTime(schedule):
        import csv

        courses_by_dept_and_meetingTime = schedule.get_courses_by_dept_and_meetingTime()
        meeting_times = sorted(schedule._data.get_meetingTimes(), key=lambda mt: mt.get_id())

        table = [['Meeting Time'] + [dept_name for dept_name in courses_by_dept_and_meetingTime.keys()]]

        for meeting_time in meeting_times:
            row = [meeting_time.get_time()]
            for dept_name in courses_by_dept_and_meetingTime.keys():
                courses = courses_by_dept_and_meetingTime[dept_name][meeting_time.get_id()]
                course_names = ", ".join([f"{course[0].get_name()} ({course[1].get_id()})" for course in courses])
                row.append(course_names)
            table.append(row)

        with open("full_schedule.csv", 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(table)

        print("Data saved to csv.")
        DisplayMgr.display_schedule_conflicts(schedule)
        print(f" Conflicts: " + str(schedule.get_fitness()) + " Gaps: " + str(schedule.gapFitness()))

    @staticmethod
    def display_schedule_instructors(schedule):
        pass # to be improved
        # import csv
        # instructors = dbMgr.get_instructors()
        # meeting_times = dbMgr.get_meetingTimes()

        # # Create a dictionary to store the meeting times for each instructor
        # instructor_meeting_times = {meeting_time.get_time(): {instructor.get_name(): [] for instructor in instructors} for meeting_time in meeting_times}

        # # Iterate over the classes in the schedule and populate the instructor_meeting_times dictionary
        # for class_obj in schedule.get_classes():
        #     instructor_name = class_obj.get_instructor().get_name()
        #     meeting_time = class_obj.get_meetingTime().get_time()
        #     instructor_meeting_times[meeting_time][instructor_name].append(class_obj.get_course().get_name())

        # # Prepare the CSV file
        # with open('schedule_instructors.csv', 'w', newline='') as csvfile:
        #     writer = csv.writer(csvfile)

        #     # Write the header row with instructor names
        #     header_row = ['Meeting Times'] + [instructor.get_name() for instructor in instructors]
        #     writer.writerow(header_row)

        #     # Write the data rows with meeting times and corresponding classes
        #     for meeting_time in meeting_times:
        #         data_row = [meeting_time.get_time()] + [", ".join(instructor_meeting_times[meeting_time.get_time()][instructor.get_name()]) for instructor in instructors]
        #         writer.writerow(data_row)
        
    @staticmethod
    def display_schedule_conflicts(schedule):
        conflictsTable = prettytable.PrettyTable(['conflict type', 'conflict between classes'])
        conflicts = schedule.get_conflicts()
        for i in range(0, len(conflicts)):
            conflictsTable.add_row([str(conflicts[i].get_conflictType()),
                                    str("  and  ".join(map(str, conflicts[i].get_conflictBetweenClasses())))])
        if (len(conflicts) > 0): print(conflictsTable)

def find_fittest_schedule(verboseFlag, process_num):
    generationNumber = 0
    print("> Generation # "+str(generationNumber))
    population = Population(POPULATION_SIZE)
    population.get_schedules().sort(key=lambda x: x.gapFitness())
    if (verboseFlag):
        DisplayMgr.display_schedule_conflicts(population.get_schedules()[0])
    geneticAlgorithm = GeneticAlgorithm()
    conflict_count = -1
    gap_count = -1
    count_repeating = 1
    fitness = population.get_schedules()[0].get_fitness()
    while (population.get_schedules()[0].get_fitness() < 1.00 and population.get_schedules()[0].gapFitness() < 0.5):
        generationNumber += 1
        if round(1/population.get_schedules()[0].get_fitness()) != conflict_count or population.get_schedules()[0].gapFitness() != gap_count:
            conflict_count = round(1/population.get_schedules()[0].get_fitness())
            gap_count = population.get_schedules()[0].gapFitness()
            count_repeating = 1
        else:
            count_repeating += 1
        global MUTATION_RATE
        print(f"\n> [{process_num}] Generation # " + str(generationNumber) + " Conflicts: " + str(round(1/population.get_schedules()[0].get_fitness()) - 1) + " Gaps: " + str(round(1/population.get_schedules()[0].gapFitness()) - 1) + ", tries " + str(count_repeating) + ", Mutation Rate: " + str(MUTATION_RATE))
        population = geneticAlgorithm.evolve(population)
        if fitness > 0.05 or gap_count < 20:
            if count_repeating % rnd.randint(2, 3) == 0:
                population.get_schedules().sort(key=lambda x: (x.gapFitness(), x.get_fitness()), reverse=True)
            else:
                population.get_schedules().sort(key=lambda x: (x.get_fitness(), x.gapFitness()), reverse=True)
        else:
            population.get_schedules().sort(key=lambda x: x.get_fitness(), reverse=True)
        
        if 999 > count_repeating > 25 and count_repeating % rnd.randint(2,5) == 0:
            if MUTATION_RATE > 0.001:
                MUTATION_RATE = MUTATION_RATE - 0.001
            else: 
                MUTATION_RATE = rnd.random() / 100
        elif count_repeating == 1:
            MUTATION_RATE = 0.005
        elif count_repeating > 999:
            break
                
    print("> solution found after " + str(generationNumber) + " generations")
    
    population.get_schedules()[0]._data._c = None
    population.get_schedules()[0]._data._conn = None
    return population.get_schedules()[0]

from concurrent.futures import ProcessPoolExecutor

def handle_command_line(verboseFlag):
    while (True):
        entry = input("> What do you want to do (i:nitial data display, f:ind fittest schedule, d:efault mode, v:erbose mode, e:xit)\n")
        if (entry == "i"): DisplayMgr.display_input_data()
        elif (entry == "f"):
            schedules = []
            workers = 3
            
            # compute multiple schedules at the same time
            with ProcessPoolExecutor(max_workers=workers) as exe:
                schedules = exe.map(find_fittest_schedule, [verboseFlag] * workers, range(0, workers))
            
            # pick the one that has the highest fitness 
            handle_schedule_display(max(schedules, key=lambda schedule: (schedule.gapFitness(), schedule.get_fitness())))

        elif (entry == "d"): verboseFlag = False
        elif (entry == "v"): verboseFlag = True
        elif (entry == "e"): break

def handle_schedule_display(schedule):
    DisplayMgr.display_schedule_conflicts(schedule)
    DisplayMgr.display_schedule_instructors(schedule)
    DisplayMgr.display_schedule_by_dept_and_meetingTime(schedule)


dbMgr = DatabaseManager()
if __name__ == '__main__':
    handle_command_line(VERBOSE_FLAG)


