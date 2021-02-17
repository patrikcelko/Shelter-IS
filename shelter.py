import datetime


def try_argument(arg, type_value):
    if not isinstance(arg, type_value):
        raise RuntimeError("Arguments doesn't match data type requirements.")


class Shelter:
    def __init__(self):
        self.animals = []
        self.fosters = []

    def add_animal(self, name, year_of_birth, gender, date_of_entry, species, breed):
        if type(name) != str or type(year_of_birth) != int or type(gender) != str or \
                not isinstance(date_of_entry, datetime.date) or type(species) != str or type(breed) != str:
            raise RuntimeError("Arguments doesn't match data type requirements.")

        for animal in self.animals:
            if animal.name == name and animal.year_of_birth == year_of_birth and animal.gender == gender and \
                    animal.species == species and animal.breed == breed:
                if animal.date_of_entry == date_of_entry:
                    return animal
                else:
                    raise RuntimeError("Detected same animal with different entry date.")

        animal = Animal(name, year_of_birth, gender, date_of_entry, species, breed, self)
        self.animals.append(animal)
        self.animals.sort(key=lambda x: x.name)
        return animal

    def get_foster_sql(self, sql_id):
        for foster in self.fosters:
            if foster.SQL_ID == sql_id:
                return foster
        return None

    def list_animals(self, date, name=None, year_of_birth=None, gender=None, date_of_entry=None,
                     species=None, breed=None):
        animals_copy = []
        for animal_copy in self.animals:
            animals_copy.append(animal_copy)

        try_argument(date, datetime.date)
        animals_copy = list(filter(lambda animal: (animal.check_if_is_in_shelter(date)), animals_copy))

        if name is not None:
            try_argument(name, str)
            animals_copy = list(filter(lambda animal: (animal.name == name), animals_copy))

        if year_of_birth is not None:
            try_argument(year_of_birth, int)
            animals_copy = list(filter(lambda animal: (animal.year_of_birth == year_of_birth), animals_copy))

        if gender is not None:
            try_argument(gender, str)
            animals_copy = list(filter(lambda animal: (animal.gender == gender), animals_copy))

        if date_of_entry is not None:
            try_argument(date_of_entry, datetime.date)
            animals_copy = list(filter(lambda animal: (animal.date_of_entry == date_of_entry), animals_copy))

        if species is not None:
            try_argument(species, str)
            animals_copy = list(filter(lambda animal: (animal.species == species), animals_copy))

        if breed is not None:
            try_argument(breed, str)
            animals_copy = list(filter(lambda animal: (animal.breed == breed), animals_copy))
        return animals_copy

    def add_foster_parent(self, name, address, phone_number, max_animals, sql_id=None):
        if type(name) != str or type(address) != str or type(phone_number) != str or type(max_animals) != int:
            raise RuntimeError("Arguments doesn't match data type requirements.")

        for foster_v in self.fosters:
            if foster_v.name == name and foster_v.address == address and foster_v.phone_number == phone_number:
                if foster_v.max_animals == max_animals:
                    return foster_v
                else:
                    raise RuntimeError("Detected same foster with different animal limit.")

        foster = Foster(name, address, phone_number, max_animals)
        if sql_id is not None:
            foster.SQL_ID = sql_id
        self.fosters.append(foster)

    def amount_of_animals_in_care(self, parent, date):
        counter = 0
        for animal in self.animals:
            if animal.foster_records is None:
                continue
            for record in animal.foster_records:
                if record.foster != parent:
                    continue

                if record.is_active(date):
                    counter += 1
        return counter

    def available_foster_parents(self, date):
        available_fosters = []
        for foster in self.fosters:
            if self.amount_of_animals_in_care(foster, date) < foster.max_animals:
                available_fosters.append(foster)
        return available_fosters


class Animal:
    def __init__(self, name, year_of_birth, gender, date_of_entry, species, breed, shelter,
                 veterinary_records=None, foster_records=None, adopter=None):
        self.shelter = shelter
        self.name = name
        self.year_of_birth = year_of_birth
        self.gender = gender
        self.date_of_entry = date_of_entry
        self.species = species
        self.breed = breed
        self.veterinary_records = veterinary_records
        self.foster_records = foster_records
        self.adopter = adopter
        self.SQL_ID = None

    def start_foster(self, date, parent, ignore=False, end=None):
        if not isinstance(date, datetime.date) or type(parent) != Foster:
            raise RuntimeError("Arguments doesn't match data type requirements.")
        if not ignore:
            if parent not in self.shelter.available_foster_parents(date):
                raise RuntimeError("Selected parent doesn't exist or reached limit for animals in selected time.")
            self.check_if_is_in_shelter_raise(date)

        if self.foster_records is None:
            self.foster_records = []
        rec = FosterRecord(parent, date)
        self.foster_records.append(rec)
        parent.animals_in_care.add(self)  # The most important line :D

        if end is not None:
            if type(end) == date:
                raise RuntimeError("Invalid end argument.")
            rec.period_to = end

    def compare_with_sql(self, data):
        if data is not None and len(data) == 6:
            if self.name == data[1] and self.year_of_birth == data[2] and self.gender == data[3] and self.species == \
                    data[4] and self.breed == data[5]:
                return True
        return False

    def end_foster(self, date):
        try_argument(date, datetime.date)
        found_valid = False
        if self.foster_records is None:
            raise RuntimeError("This animal isn't in foster care.")

        for record in self.foster_records:
            if record.period_to is None:
                found_valid = True
                record.period_to = date
                break

        if not found_valid:
            raise RuntimeError("This animal isn't in foster care.")

    def adopt(self, date, adopter_name, adopter_address):
        if not isinstance(date, datetime.date) or type(adopter_address) != str or type(adopter_name) != str:
            raise RuntimeError("Arguments doesn't match data type requirements.")
        self.check_if_is_in_shelter_raise(date)
        self.adopter = Adopter(adopter_name, adopter_address), date

    def check_if_is_in_shelter_raise(self, date):
        if not self.check_if_is_in_shelter(date):
            raise RuntimeError("This animal is now in foster care or adopted.")

    def check_if_is_in_shelter(self, date):
        if self.date_of_entry > date:
            return False

        if self.adopter is not None and self.adopter[1] <= date:
            return False

        if self.foster_records is None:
            return True

        for record in self.foster_records:
            if record.is_active(date):
                return False
        return True

    def add_exam(self, vet, date, report):
        if type(vet) != str or not isinstance(date, datetime.date) or type(report) != str:
            raise RuntimeError("Arguments doesn't match data type requirements.")
        self.check_if_is_in_shelter_raise(date)

        if self.veterinary_records is None:
            self.veterinary_records = []
        self.veterinary_records.append(VetRecord(vet, date, report))

    def list_exams(self, start, end):
        copy_veterinary_records = []
        if self.veterinary_records is not None:
            for record in self.veterinary_records:
                copy_veterinary_records.append(record)

            if start is not None:
                try_argument(start, datetime.date)
                copy_veterinary_records = list(filter(lambda rec: (rec.date >= start), copy_veterinary_records))

            if end is not None:
                try_argument(end, datetime.date)
                copy_veterinary_records = list(filter(lambda rec: (rec.date <= end), copy_veterinary_records))
        return copy_veterinary_records


class FosterRecord:
    def __init__(self, foster, period_from):
        self.foster = foster
        self.period_from = period_from
        self.period_to = None

    def is_active(self, date):
        return (self.period_to is None and self.period_from <= date) or \
               (self.period_to is not None and self.period_to >= date >= self.period_from)


class Foster:
    def __init__(self, name, address, phone_number, max_animals=1):
        self.name = name
        self.address = address
        self.phone_number = phone_number
        self.max_animals = max_animals
        self.animals_in_care = set()
        self.SQL_ID = None

    def compare_foster_with_sql(self, data):
        if data is not None and len(data) == 4:
            if self.name == data[1] and self.address == data[2] and self.phone_number == data[3]:
                return True
        return False


class Adopter:
    def __init__(self, name, address):
        self.name = name
        self.address = address


class VetRecord:
    def __init__(self, vet, date, report):
        self.date = date
        self.vet = vet
        self.report = report


def make_test_shelter():
    shelter = Shelter()
    date1 = datetime.date(2018, 6, 1)
    date2 = datetime.date(2010, 6, 1)
    date3 = datetime.date(2010, 6, 2)

    shelter.add_animal('Marry', 1999, 'male', date1, 'dog', 'labrador')
    shelter.add_animal('Adam', 2000, 'female', date2, 'rat', 'small')
    shelter.add_animal('James', 1980, 'female', date3, 'rat', 'big')

    return date1, date2, date3, shelter


def add_foster_parents(sh: Shelter) -> None:
    sh.add_foster_parent('Boris', 'New York', '12345', 3)
    sh.add_foster_parent('Lena', 'Moscow', '123456', 1)
    sh.add_foster_parent('Roman', 'Prague', '1234567', 0)


def test_add_animal():
    shelter = Shelter()
    shelter.add_animal('Pleb', 1999, 'male', datetime.date(2018, 6, 1), 'dog', 'labrador')


def test_list_animals_basic():
    date1, date2, date3, shelter = make_test_shelter()

    animals = shelter.list_animals(date2)
    assert len(animals) == 1
    assert animals[0].name == 'Adam'

    animals = sorted(shelter.list_animals(date3), key=lambda x: x.name)
    assert len(animals) == 2
    assert animals[0].name == 'Adam'
    assert animals[1].name == 'James'

    animals = sorted(shelter.list_animals(date1), key=lambda x: x.name)
    assert len(animals) == 3
    assert animals[0].name == 'Adam'
    assert animals[1].name == 'James'
    assert animals[2].name == 'Marry'

    animals = sorted(shelter.list_animals(date1, species='rat'), key=lambda x: x.name)
    assert len(animals) == 2
    assert animals[0].name == 'Adam'
    assert animals[1].name == 'James'

    animals = sorted(shelter.list_animals(date1, species='rat', gender='female'), key=lambda x: x.name)
    assert len(animals) == 2
    assert animals[0].name == 'Adam'
    assert animals[1].name == 'James'

    animals = shelter.list_animals(date1, year_of_birth=1999)
    assert len(animals) == 1
    assert animals[0].name == 'Marry'


def test_exam_basic():
    date1, date2, date3, shelter = make_test_shelter()
    date_ex1 = datetime.date(2019, 6, 1)
    date_ex2 = datetime.date(2011, 6, 2)
    animals = sorted(shelter.list_animals(date1, species='rat'), key=lambda x: x.name)

    animals[0].add_exam('Jan', date_ex1, 'Bald spot in fur examined.')
    animals[1].add_exam('Freddy', date_ex2, 'Routine entrance exam.')
    animals[1].add_exam('Lessie', date_ex1, 'Bald spot in fur examined.')

    assert len(animals[0].veterinary_records) == 1
    assert animals[0].veterinary_records[0].vet == 'Jan'
    assert animals[0].veterinary_records[0].date == date_ex1

    assert len(animals[1].veterinary_records) == 2
    assert animals[1].veterinary_records[0].date == date_ex2

    animals_ex = sorted(animals[1].list_exams(None, None), key=lambda x: x.date)
    assert len(animals_ex) == 2
    assert animals_ex[0].report == 'Routine entrance exam.'
    assert animals_ex[1].report == 'Bald spot in fur examined.'

    animals_ex = sorted(animals[1].list_exams(date2, None), key=lambda x: x.date)
    assert len(animals_ex) == 2
    assert animals_ex[0].report == 'Routine entrance exam.'
    assert animals_ex[1].report == 'Bald spot in fur examined.'

    animals_ex = sorted(animals[1].list_exams(date_ex1, None), key=lambda x: x.date)
    assert len(animals_ex) == 1
    assert animals_ex[0].report == 'Bald spot in fur examined.'

    date_bad1 = datetime.date(2011, 6, 3)
    date_bad2 = datetime.date(2011, 6, 4)
    animals_ex = sorted(animals[1].list_exams(date_bad1, date_bad2), key=lambda x: x.date)
    assert len(animals_ex) == 0

    animals_ex = sorted(animals[1].list_exams(date_ex1, date_ex1), key=lambda x: x.date)
    assert len(animals_ex) == 1
    assert animals_ex[0].report == 'Bald spot in fur examined.'

    date_ex1_less = datetime.date(2018, 10, 10)
    animals_ex = sorted(animals[1].list_exams(date_ex1_less, date_ex1), key=lambda x: x.date)
    assert len(animals_ex) == 1
    assert animals_ex[0].report == 'Bald spot in fur examined.'


def test_adopt_basic():
    date1, date2, date3, shelter = make_test_shelter()
    animals = sorted(shelter.list_animals(date1, species='rat'), key=lambda x: x.name)
    date_ad1 = datetime.date(2020, 6, 1)
    animals[0].adopt(date_ad1, 'Patrick', 'Prague')


def test_foster_parents_basic():
    date1, date2, date3, shelter = make_test_shelter()
    add_foster_parents(shelter)
    fosters = sorted(shelter.available_foster_parents(date1), key=lambda x: x.name)

    assert len(fosters) == 2
    assert fosters[0].phone_number == '12345'
    assert fosters[1].phone_number == '123456'


def test_foster_care_basic():
    date1, date2, date3, shelter = make_test_shelter()
    animals = sorted(shelter.list_animals(date1, species='rat'), key=lambda x: x.name)
    add_foster_parents(shelter)
    fosters = sorted(shelter.available_foster_parents(date1), key=lambda x: x.name)
    animals[0].start_foster(date1, fosters[0])
    animals[0].end_foster(date1)


def test_foster_care_error():
    date1, date2, date3, shelter = make_test_shelter()
    animals = sorted(shelter.list_animals(date1, species='rat'), key=lambda x: x.name)

    try:
        animals[0].end_foster(date1)
        raise AssertionError()
    except RuntimeError:
        pass

    date1, date2, date3, shelter = make_test_shelter()
    animals = sorted(shelter.list_animals(date1, species='rat'), key=lambda x: x.name)
    add_foster_parents(shelter)
    fosters = sorted(shelter.available_foster_parents(date1), key=lambda x: x.name)

    try:
        animals[0].start_foster(date2, fosters[0])
        animals[0].start_foster(date2, fosters[1])
        raise AssertionError()
    except RuntimeError:
        pass


def test_adoption_foster_error():
    date1, date2, date3, shelter = make_test_shelter()
    animals = sorted(shelter.list_animals(date1, species='rat'), key=lambda x: x.name)
    add_foster_parents(shelter)
    fosters = sorted(shelter.available_foster_parents(date1), key=lambda x: x.name)

    try:
        animals[0].start_foster(date2, fosters[0])
        animals[0].adopt(date1, 'Marry', 'New York')
        raise AssertionError()
    except RuntimeError:
        pass

    date1, date2, date3, shelter = make_test_shelter()
    animals = sorted(shelter.list_animals(date1, species='rat'), key=lambda x: x.name)
    add_foster_parents(shelter)
    fosters = sorted(shelter.available_foster_parents(date1), key=lambda x: x.name)

    try:
        animals[0].adopt(date3, 'Henry', 'Epstein city')
        animals[0].start_foster(date1, fosters[0])
        raise AssertionError()
    except RuntimeError:
        pass


def test_exam_error():
    date1, date2, date3, shelter = make_test_shelter()
    animals = sorted(shelter.list_animals(date1, species='rat'), key=lambda x: x.name)
    add_foster_parents(shelter)
    fosters = sorted(shelter.available_foster_parents(date1), key=lambda x: x.name)

    try:
        animals[0].start_foster(date2, fosters[0])
        animals[0].add_exam('Pro Care', date1, 'Thorough entrance exam.')
        raise AssertionError()
    except RuntimeError:
        pass

    try:
        animals[1].adopt(date3, 'Adolf', 'Berlin')
        animals[1].add_exam('Aut Care', date1, 'Routine entrance exam.')
        raise AssertionError()
    except RuntimeError:
        pass


def test_foster_cap_exceeded():
    date1, date2, date3, shelter = make_test_shelter()
    animals = sorted(shelter.list_animals(date1, species='rat'), key=lambda x: x.name)
    add_foster_parents(shelter)
    fosters = sorted(shelter.available_foster_parents(date1), key=lambda x: x.name)

    try:
        animals[0].start_foster(date2, fosters[1])
        animals[1].start_foster(date1, fosters[1])
        raise AssertionError()
    except RuntimeError:
        pass

    animals[0].end_foster(date3)
    animals[1].start_foster(date1, fosters[1])


def test_sanity_fail():
    s = Shelter()
    brute = s.add_animal(name='Brute', year_of_birth=2014, gender='M', date_of_entry=datetime.date(2016, 11, 23),
                         species='platypus', breed='unknown')
    brute.add_exam('Mark Care', datetime.date(2016, 11, 24), 'Routine entrance exam.')
    brute.add_exam('Pro Care', datetime.date(2017, 1, 3), 'Bald spot in fur examined.')

    rex = s.add_animal(name='Rex', year_of_birth=2020, gender='M', date_of_entry=datetime.date(2020, 7, 7),
                       species='dog', breed='Shepherd')
    rex.add_exam('Antonin Care', datetime.date(2020, 9, 7), 'Thorough entrance exam.')

    assert len(rex.list_exams(start=datetime.date(2014, 1, 1), end=datetime.date(2020, 10, 1))) == 1


def test_main_shelter():
    test_add_animal()
    test_list_animals_basic()
    test_exam_basic()
    test_adopt_basic()
    test_foster_parents_basic()
    test_foster_care_basic()
    test_foster_care_error()
    test_adoption_foster_error()
    test_exam_error()
    test_foster_cap_exceeded()
    test_sanity_fail()


if __name__ == '__main__':
    test_main_shelter()
