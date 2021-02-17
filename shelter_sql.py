import json
import random
import sqlite3
import string
from datetime import date

import shelter_json
from shelter import *


def init_db(db: sqlite3.Connection) -> None:
    # animals - > ID_animal*, name, year_of_birth, gender, species, breed
    db.execute("CREATE TABLE IF NOT EXISTS animals (ID_animal INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, "
               "year_of_birth INTEGER NOT NULL, gender TEXT NOT NULL, species TEXT NOT NULL, breed TEXT NOT NULL);")

    # fosters -> ID_foster*, name, address, phone
    db.execute("CREATE TABLE IF NOT EXISTS fosters (ID_foster INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL, "
               "address TEXT NOT NULL, phone TEXT NOT NULL);")

    # snapshots -> ID_snapshot*, snap_d
    db.execute("CREATE TABLE IF NOT EXISTS snapshots (ID_snapshot INTEGER NOT NULL PRIMARY KEY, snap_d DATE NOT NULL);")

    # foster_record -> ID_foster*, ID_animal*, ID_snapshot*, period_from, period_to
    db.execute("CREATE TABLE IF NOT EXISTS foster_record (ID_foster INTEGER NOT NULL REFERENCES fosters(ID_foster), "
               "ID_animal INTEGER NOT NULL REFERENCES animals(ID_animal), ID_snapshot INTEGER NOT NULL REFERENCES "
               "snapshots(ID_snapshot), period_from DATE NOT NULL, period_to DATE);")

    # adopter -> ID_animal*, ID_snapshot*, ID_adopter*, date
    db.execute("CREATE TABLE IF NOT EXISTS adopter (ID_animal INTEGER NOT NULL REFERENCES animals(ID_animal), "
               "ID_snapshot INTEGER NOT NULL REFERENCES snapshots(ID_snapshot), ID_adopter INTEGER NOT NULL "
               "REFERENCES adopter_human(ID_adopter), date_adopt DATE NOT NULL, unique(ID_animal, ID_snapshot, "
               "ID_adopter));")

    # vet_record -> ID_animal*, ID_snapshot*, vet, vet_date, report
    db.execute("CREATE TABLE IF NOT EXISTS vet_record (ID_animal INTEGER NOT NULL REFERENCES animals(ID_animal), "
               "ID_snapshot INTEGER NOT NULL REFERENCES snapshots(ID_snapshot), vet TEXT NOT NULL, vet_date DATE NOT "
               "NULL, report TEXT NOT NULL);")

    # snapshot_entry -> ID_animal*, ID_snapshot*, date_of_entry
    db.execute("CREATE TABLE IF NOT EXISTS snapshot_entry (ID_animal INTEGER NOT NULL REFERENCES animals(ID_animal), "
               "ID_snapshot INTEGER NOT NULL REFERENCES snapshots(ID_snapshot), date_of_entry DATE NOT NULL, "
               "unique(ID_animal, ID_snapshot));")

    # snapshot_max_animals -> ID_foster*, ID_snapshot*, max_animals
    db.execute("CREATE TABLE IF NOT EXISTS snapshot_max_animals (ID_foster INTEGER NOT NULL REFERENCES fosters("
               "ID_foster), ID_snapshot INTEGER NOT NULL REFERENCES snapshots(ID_snapshot), max_animals INTEGER NOT "
               "NULL, unique(ID_foster, ID_snapshot));")

    # adopter_human -> ID_adopter*, name, address
    db.execute("CREATE TABLE IF NOT EXISTS adopter_human (ID_adopter INTEGER NOT NULL PRIMARY KEY, name TEXT NOT "
               "NULL, address TEXT NOT NULL, unique(ID_adopter, name, address) )")


def create_snapshot(db: sqlite3.Connection, shelter: Shelter) -> int:
    cursor: sqlite3.Cursor = db.cursor()
    cursor.execute("INSERT INTO snapshots VALUES (NULL, ?);", (datetime.datetime.now().strftime("%Y-%m-%d"),))
    snapshot_ID: int = cursor.lastrowid

    for foster in shelter.fosters:
        cursor.execute("SELECT ID_foster FROM fosters WHERE name = ? AND address = ? AND phone = ?;",
                       (foster.name, foster.address, foster.phone_number,))
        result = cursor.fetchall()
        if len(result) == 1:
            foster_ID: int = result[0][0]
        else:
            cursor.execute("INSERT INTO fosters VALUES (NULL, ?, ?, ?)",
                           (foster.name, foster.address, foster.phone_number,))
            foster_ID = cursor.lastrowid
        foster.SQL_ID = foster_ID
        cursor.execute("INSERT INTO snapshot_max_animals VALUES(?, ?, ?)",
                       (str(foster_ID), str(snapshot_ID), str(foster.max_animals)))

    for animal in shelter.animals:
        cursor.execute("SELECT ID_animal FROM animals WHERE name = ? AND year_of_birth = ? AND gender = ? AND species "
                       "= ? AND breed = ?;",
                       (animal.name, str(animal.year_of_birth), animal.gender, animal.species, animal.breed,))
        result = cursor.fetchall()
        if len(result) == 1:
            animal_ID: int = result[0][0]
        else:
            cursor.execute("INSERT INTO animals VALUES (NULL, ?, ?, ?, ?, ?);",
                           (animal.name, str(animal.year_of_birth), animal.gender, animal.species, animal.breed,))
            animal_ID = cursor.lastrowid
        cursor.execute("INSERT INTO snapshot_entry VALUES (?, ?, ?);",
                       (str(animal_ID), str(snapshot_ID), str(animal.date_of_entry),))
        for rec in (animal.veterinary_records if animal.veterinary_records is not None else []):
            cursor.execute("INSERT INTO vet_record VALUES (?, ?, ?, ?, ?);",
                           (str(animal_ID), str(snapshot_ID), rec.vet, str(rec.date), rec.report,))
        if animal.adopter is not None:
            cursor.execute("SELECT ID_adopter FROM adopter_human WHERE name = ? AND address = ?;",
                           (animal.adopter[0].name, animal.adopter[0].address,))
            result_adopter = cursor.fetchall()
            if len(result_adopter) == 1:
                adopter_id = result_adopter[0][0]
            else:
                cursor.execute("INSERT INTO adopter_human VALUES (NULL, ?, ?);",
                               (animal.adopter[0].name, animal.adopter[0].address,))
                adopter_id = cursor.lastrowid
            cursor.execute("INSERT INTO adopter VALUES(?, ?, ?, ?);",
                           (str(animal_ID), str(snapshot_ID), str(adopter_id), animal.adopter[1]))
        for rec in (animal.foster_records if animal.foster_records is not None else []):
            cursor.execute("INSERT INTO foster_record VALUES (?, ?, ?, ?, ?);",
                           (str(rec.foster.SQL_ID), str(animal_ID), str(snapshot_ID),
                            str(rec.period_from),
                            (str(rec.period_to) if rec.period_to is not None else "NULL"),))
    db.commit()
    return snapshot_ID


def store(shelter: Shelter, db: sqlite3.Connection, deduplicate: bool = False) -> int:
    if type(db) != sqlite3.Connection:
        raise RuntimeError("Invalid database connector.")
    if type(shelter) != Shelter:
        raise RuntimeError("Invalid shelter object type.")
    init_db(db)
    if not deduplicate:
        return create_snapshot(db, shelter)
    else:
        return deduplicate_db(db, shelter)


def try_match_vet_records(animal: Animal, snapshot_id: int, animal_id: int, cursor: sqlite3.Cursor) -> bool:
    cursor.execute("SELECT * FROM vet_record WHERE ID_animal = ? AND ID_snapshot = ?;",
                   (str(animal_id), str(snapshot_id),))
    vet_records_result = cursor.fetchall()
    if len(vet_records_result) == 0 and animal.veterinary_records is None:
        return True

    if (len(vet_records_result) > 0 and animal.veterinary_records is None) or \
            (len(vet_records_result) == 0 and animal.veterinary_records is not None):
        return False

    if animal.veterinary_records is not None and len(vet_records_result) != len(animal.veterinary_records):
        return False

    shelter_records = animal.veterinary_records[:]
    while vet_records_result:
        record_sql = vet_records_result.pop()
        found_record = None
        for record_index in range(len(shelter_records)):
            if shelter_records[record_index].vet == record_sql[2] and \
                    str(shelter_records[record_index].date) == record_sql[3] and \
                    shelter_records[record_index].report == record_sql[4]:
                found_record = record_index
                break
        if found_record is None:
            return False
        shelter_records.pop(found_record)
    return len(shelter_records) == 0


def try_match_adopter_records(animal: Animal, snapshot_id: int, animal_id: int, cursor: sqlite3.Cursor) -> bool:
    cursor.execute("SELECT * FROM adopter WHERE ID_animal = ? AND ID_snapshot = ?;",
                   (str(animal_id), str(snapshot_id),))
    animal_adoption_result = cursor.fetchall()
    if len(animal_adoption_result) == 0 and animal.adopter is None:
        return True
    if (len(animal_adoption_result) == 1 and animal.adopter is None) or \
            (len(animal_adoption_result) == 0 and animal.adopter is not None):
        return False
    cursor.execute("SELECT * FROM adopter_human WHERE ID_adopter = ?;", (str(animal_adoption_result[0][2]),))

    animal_adopter_data = cursor.fetchone()

    return (str(animal.adopter[1]) == animal_adoption_result[0][3] and animal_adopter_data[1] == animal.adopter[0].name
            and animal.adopter[0].address == animal_adopter_data[2])


def try_associate_animals_ID(shelter: Shelter, snapshot_id: int, cursor: sqlite3.Cursor) -> bool:
    cursor.execute("SELECT ID_animal, date_of_entry FROM snapshot_entry WHERE ID_snapshot = ?;", (str(snapshot_id),))
    animals_data_result = cursor.fetchall()
    if len(shelter.animals) != len(animals_data_result):
        return False
    problem_found: bool = False

    for (animal_ID, date,) in animals_data_result:
        if problem_found:
            return False
        for shelter_animal in shelter.animals:
            if str(shelter_animal.date_of_entry) != date:
                continue

            cursor.execute("SELECT * FROM animals WHERE ID_animal = ?;", (str(animal_ID),))
            result_animal = cursor.fetchone()
            if shelter_animal.compare_with_sql(result_animal):
                if not try_match_vet_records(shelter_animal, snapshot_id, animal_ID, cursor) or \
                        not try_match_adopter_records(shelter_animal, snapshot_id, animal_ID, cursor):
                    continue
                shelter_animal.SQL_ID = animal_ID
                break
        else:
            problem_found = True
    return not problem_found


def try_associate_fosters_ID(shelter: Shelter, snapshot_id: int, cursor: sqlite3.Cursor) -> bool:
    cursor.execute("SELECT ID_foster, max_animals FROM snapshot_max_animals WHERE ID_snapshot = ?;",
                   (str(snapshot_id),))
    fosters_data_result = cursor.fetchall()
    if len(shelter.fosters) != len(fosters_data_result):
        return False
    problem_found: bool = False
    for (foster_ID, max_animals,) in fosters_data_result:
        if problem_found:
            return False

        for shelter_foster in shelter.fosters:
            if shelter_foster.max_animals != max_animals:
                continue

            cursor.execute("SELECT * FROM fosters WHERE ID_foster = ?;", (str(foster_ID),))
            if shelter_foster.compare_foster_with_sql(cursor.fetchone()):
                shelter_foster.SQL_ID = foster_ID
                break
        else:
            problem_found = True
    return not problem_found


def try_associate_foster_records(shelter: Shelter, snapshot_id: int, cursor: sqlite3.Cursor) -> bool:
    for animal in shelter.animals:
        cursor.execute("SELECT * FROM foster_record WHERE ID_animal = ? AND ID_snapshot = ?;",
                       (str(animal.SQL_ID), str(snapshot_id)))
        records_sql = cursor.fetchall()
        if len(records_sql) == 0 and animal.foster_records is None:
            continue
        if (len(records_sql) > 0 and animal.foster_records is None) or \
                (len(records_sql) == 0 and animal.foster_records is not None):
            return False

        shelter_records = animal.foster_records[:]
        while records_sql:
            record_sql = records_sql.pop()
            found_record = None
            for record_index in range(len(shelter_records)):
                if shelter_records[record_index].foster.SQL_ID == record_sql[0] and \
                        str(shelter_records[record_index].period_from) == record_sql[3]:
                    if (shelter_records[record_index].period_to is None and
                        (record_sql[4] == "NULL" or record_sql[4] is None)) or \
                            (shelter_records[record_index].period_to is not None
                             and str(shelter_records[record_index].period_to) == record_sql[4]):
                        found_record = record_index
                        break
            if found_record is None:
                return False
            shelter_records.pop(found_record)
    return True


def deduplicate_db(db: sqlite3.Connection, shelter: Shelter) -> int:
    cursor: sqlite3.Cursor = db.cursor()
    cursor.execute("SELECT ID_snapshot from snapshots;")
    for (snapshot_ID,) in cursor.fetchall():
        if not try_associate_animals_ID(shelter, snapshot_ID, cursor):
            continue
        if not try_associate_fosters_ID(shelter, snapshot_ID, cursor):
            continue
        if not try_associate_foster_records(shelter, snapshot_ID, cursor):
            continue
        return snapshot_ID
    return create_snapshot(db, shelter)


def load(id, db: sqlite3.Connection) -> Shelter:
    if type(db) != sqlite3.Connection:
        raise RuntimeError("Invalid database connector.")
    if type(id) != int:
        raise RuntimeError("Invalid ID type.")
    init_db(db)
    shelter = Shelter()
    cursor = db.cursor()
    cursor.execute("SELECT ID_foster, max_animals FROM snapshot_max_animals WHERE ID_snapshot = ?;", (str(id),))
    for (foster_id, max_animals,) in cursor.fetchall():
        cursor.execute("SELECT * FROM fosters WHERE ID_foster = ?;", (str(foster_id),))
        foster_data = cursor.fetchone()
        shelter.add_foster_parent(foster_data[1], foster_data[2], foster_data[3], max_animals, foster_id)
    cursor.execute("SELECT ID_animal, date_of_entry FROM snapshot_entry WHERE ID_snapshot = ?;", (str(id),))
    for (animal_id, entry_date,) in cursor.fetchall():
        cursor.execute("SELECT * FROM animals WHERE ID_animal = ?;", (animal_id,))
        animal_data = cursor.fetchone()
        animal_obj = shelter.add_animal(
            animal_data[1], animal_data[2], animal_data[3],
            datetime.date.fromisoformat(entry_date), animal_data[4], animal_data[5])
        animal_obj.SQL_ID = animal_id

        cursor.execute("SELECT * FROM vet_record WHERE ID_animal = ? AND ID_snapshot = ?;", (str(animal_id), str(id),))
        for rec in cursor.fetchall():
            animal_obj.add_exam(rec[2], datetime.date.fromisoformat(rec[3]), rec[4])

        cursor.execute("SELECT * FROM foster_record WHERE ID_animal = ? AND ID_snapshot = ?;",
                       (str(animal_id), str(id),))
        for rec in cursor.fetchall():
            foster = shelter.get_foster_sql(rec[0])
            if foster is None:
                raise RuntimeError("Integrity err.")

            if rec[4] != "NULL" and rec[4] is not None:
                animal_obj.start_foster(datetime.date.fromisoformat(rec[3]), foster, True,
                                        datetime.date.fromisoformat(rec[4]))
            else:
                animal_obj.start_foster(datetime.date.fromisoformat(rec[3]), foster, True)

        cursor.execute("SELECT * FROM adopter WHERE ID_animal = ? AND ID_snapshot = ?;", (str(animal_id), str(id),))
        adopt_result = cursor.fetchall()
        if len(adopt_result) == 1:
            cursor.execute("SELECT * FROM adopter_human WHERE ID_adopter = ?;", (adopt_result[0][2],))
            adopter_data = cursor.fetchone()
            animal_obj.adopt(datetime.date.fromisoformat(adopt_result[0][3]), adopter_data[1], adopter_data[2])
    return shelter


def make_test_shelterU():
    shelter = Shelter()
    date1 = datetime.date(2003, 6, 1)
    shelter.add_animal('Adam', 1900, 'male', date1, 'dog', 'labrador')

    date2 = datetime.date(2001, 6, 1)
    shelter.add_animal('Johny', 2000, 'female', date2, 'rat', 'small')

    date3 = datetime.date(1996, 6, 3)
    shelter.add_animal('Gustav Big', 1980, 'female', date3, 'rat', 'big')

    shelter.add_foster_parent('Freddy', 'New York', '09542985248', 3)
    shelter.add_foster_parent('Pleb1', 'Sydney', '03658485851', 5)
    shelter.add_foster_parent('Unknown', 'Moscow', '02254852928', 2)
    return date1, date2, date3, shelter


def unlink_and_make_db(filename: str, unlink: bool = True):
    import os
    import errno
    try:
        if unlink:
            os.unlink(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
    return sqlite3.connect(filename)


def test_store_shelter(db: sqlite3.Connection):
    date1, date2, date3, shelter = make_test_shelterU()

    fosters = sorted(shelter.available_foster_parents(date1), key=lambda x: x.name)
    animals = shelter.list_animals(date3)
    assert len(animals) == 1

    animals[0].add_exam('Pro Care', datetime.date(2020, 9, 7), 'Thorough entrance exam.')
    animals[0].add_exam('Care Plus', datetime.date(2020, 8, 6), 'Suicide injection.')
    animals[0].add_exam('Pro Care', datetime.date(2020, 9, 7), 'Thorough entrance exam.')
    animals[0].add_exam('Care Plus', datetime.date(2020, 8, 6), 'Suicide injection.')

    animals[0].start_foster(datetime.date(2000, 9, 6), fosters[0])
    animals[0].end_foster(datetime.date(2001, 9, 5))
    animals[0].start_foster(datetime.date(2001, 9, 6), fosters[0])
    animals[0].end_foster(datetime.date(2002, 9, 6))
    animals[0].start_foster(datetime.date(2004, 9, 6), fosters[0])
    animals[0].end_foster(datetime.date(2007, 9, 6))
    shelter.animals[1].adopt(datetime.date(2040, 9, 6), 'Patrick', 'Prague')
    return shelter


def test_fail1():
    right = sqlite3.connect('shelter.dat')
    left = sqlite3.connect('shelter.dat')

    shelter = get_massive_shelter()
    ident_1_l = store(shelter, db=left)
    ident_1_r = store(shelter, db=right)
    ident_2_l = store(shelter, db=left, deduplicate=True)
    assert ident_1_l == ident_2_l
    test_json_sql(shelter, True)


def test_fail2():
    conn = sqlite3.connect('conn.dat')
    shelter = get_massive_shelter()
    ident = store(shelter, db=conn)
    shelter_2 = load(ident, db=conn)
    ident_2 = store(shelter_2, db=conn, deduplicate=True)
    ident_3 = store(shelter, db=conn, deduplicate=True)
    assert ident == ident_2

    if len(shelter.animals) == 0:
        return

    random_dates = []

    for i in range(10):
        random_dates.append(random.choice(shelter.animals).date_of_entry)

    for when in random_dates:
        animals_r = shelter.list_animals(when)
        fosters_r = shelter.available_foster_parents(when)
        shelter_json_1_animal = shelter_json.store(animals_r)
        shelter_json_1_fosters = shelter_json.store(fosters_r)

        assert len(json.loads(shelter_json_1_animal)) == len(animals_r)
        assert len(json.loads(shelter_json_1_fosters)) == len(fosters_r)

        assert len(json.loads(shelter_json_1_animal)) == len(animals_r)
        assert len(json.loads(shelter_json_1_fosters)) == len(fosters_r)


def test_sanity_fail1():
    s = Shelter()
    left = sqlite3.connect('shelter.dat')
    right = sqlite3.connect('shelter.dat')
    matilda = s.add_animal(name='Matilda', year_of_birth=2012, gender='F', date_of_entry=date(2016, 1, 27),
                           species='dog', breed='Chow chow')
    matilda.add_exam(vet='Philippe Ruben', date=date(2016, 2, 1), report="entrance exam")
    id_1_l = store(s, db=left)
    test_json_sql(s)
    id_1_r = store(s, db=right)
    s.add_foster_parent(name='Martin LeBrou', address="South Street 7A, Linz", phone_number="204 436 668",
                        max_animals=2)
    martin, = s.available_foster_parents(date=date(2020, 2, 3))
    matilda.start_foster(date=date(2020, 2, 3), parent=martin)
    id_2_l = store(s, db=left, deduplicate=True)
    id_2_r = store(s, db=right, deduplicate=True)
    test_json_sql(s)
    s_1 = load(id_1_l, db=left)
    matilda, = s.list_animals(date=date(2019, 7, 10))
    exam, = matilda.list_exams(start=None, end=None)
    assert len(s_1.available_foster_parents(date=date(2019, 7, 10))) == 0


def random_date(start, end):
    from datetime import timedelta

    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)


def get_massive_shelter():
    big_shelter = Shelter()
    letters = string.ascii_lowercase

    # Random animals
    animal_limit = random.randint(0, 100)
    print("  Generated animals:", animal_limit)
    for _ in range(0, animal_limit):
        big_shelter.add_animal(''.join(random.choice(letters) for i in range(10)),
                               random.randint(1900, 2010),
                               ''.join(random.choice(letters) for i in range(3)),
                               random_date(datetime.date(1900, 9, 6), datetime.date(2025, 9, 6)),
                               ''.join(random.choice(letters) for i in range(4)),
                               ''.join(random.choice(letters) for i in range(5)))

    # Random fosters
    fosters_limit = random.randint(0, 50)
    print("  Generated fosters:", fosters_limit)
    for _ in range(0, fosters_limit):
        big_shelter.add_foster_parent(''.join(random.choice(letters) for i in range(10)),
                                      ''.join(random.choice(letters) for i in range(20)),
                                      str(random.randint(0, 10)),
                                      random.randint(0, 50))

    # Random exams
    exams_limit = random.randint(0, 100)
    print("  Generated exams:", exams_limit)
    for _ in range(0, exams_limit):
        if len(big_shelter.animals) == 0:
            break
        random_animal = random.choice(big_shelter.animals)
        random_animal.add_exam(''.join(random.choice(letters) for i in range(5)),
                               random_date(random_animal.date_of_entry, datetime.date(2050, 9, 6)),
                               ''.join(random.choice(letters) for i in range(25)))

    # Foster care with end
    foster_care_end = random.randint(0, 25)
    print("  Generated cares with end:", foster_care_end)
    for _ in range(0, foster_care_end):
        if len(big_shelter.animals) == 0 or len(big_shelter.fosters) == 0:
            break
        random_animal = random.choice(big_shelter.animals)
        random_date_val = random_date(random_animal.date_of_entry, datetime.date(2050, 9, 6))
        if not random_animal.check_if_is_in_shelter(random_date_val):
            continue
        fosters = random_animal.shelter.available_foster_parents(random_date_val)
        if len(fosters) == 0:
            continue
        random_animal.start_foster(random_date_val, random.choice(fosters))
        random_animal.end_foster(random_date(random_date_val, datetime.date(2055, 9, 6)))

    # Foster care without end
    foster_care_no_end = random.randint(0, 20)
    print("  Generated cares without end:", foster_care_no_end)
    for _ in range(0, foster_care_no_end):
        if len(big_shelter.animals) == 0 or len(big_shelter.fosters) == 0:
            break
        random_animal = random.choice(big_shelter.animals)
        random_date_val = random_date(random_animal.date_of_entry, datetime.date(2050, 9, 6))
        if not random_animal.check_if_is_in_shelter(random_date_val):
            continue
        fosters = random_animal.shelter.available_foster_parents(random_date_val)
        if len(fosters) == 0:
            continue
        random_animal.start_foster(random_date_val, random.choice(fosters))

    # Random adoption
    adoptions_limit = random.randint(0, 5)
    print("  Generated adoptions:", adoptions_limit)
    for _ in range(0, adoptions_limit):
        if len(big_shelter.animals) == 0:
            break
        random_animal = random.choice(big_shelter.animals)
        random_date_val = random_date(random_animal.date_of_entry, datetime.date(2050, 9, 6))
        if not random_animal.check_if_is_in_shelter(random_date_val):
            continue
        random_animal.adopt(random_date_val,
                            ''.join(random.choice(letters) for i in range(5)),
                            ''.join(random.choice(letters) for i in range(15)))

    return big_shelter


def test_basic_empty():
    left = unlink_and_make_db("shelter.dat", True)
    right = unlink_and_make_db("shelter.dat", False)

    shelter = Shelter()
    ident_1_l = store(shelter, db=left)
    ident_1_r = store(shelter, db=right)
    ident_2_l = store(shelter, db=left, deduplicate=True)
    assert ident_1_l == ident_2_l
    test_json_sql(shelter)


def test_basic1():
    left = unlink_and_make_db("shelter.dat", True)
    right = unlink_and_make_db("shelter.dat", False)

    shelter = Shelter()
    ident_1_l = store(shelter, db=left)
    shelter.add_foster_parent("Jury", "New York", "02355975", 2)
    ident_1_r = store(shelter, db=right)
    test_json_sql(shelter)
    ident_2_l = store(shelter, db=left, deduplicate=True)
    assert ident_1_l != ident_2_l and ident_2_l == ident_1_r
    test_json_sql(shelter)

    left.close()
    right.close()

    left = unlink_and_make_db("shelter.dat", True)
    right = unlink_and_make_db("shelter.dat", False)

    shelter = Shelter()
    ident_1_l = store(shelter, db=left)
    shelter.add_animal(name='Matilda', year_of_birth=2012, gender='F', date_of_entry=date(2016, 1, 27), species='dog',
                       breed='Chow chow')
    ident_1_r = store(shelter, db=right)
    ident_2_l = store(shelter, db=left, deduplicate=True)
    assert ident_1_l != ident_2_l and ident_2_l == ident_1_r
    test_json_sql(shelter)


def test_basic_leftovers1():
    left = unlink_and_make_db("shelter.dat", True)
    right = unlink_and_make_db("shelter.dat", False)

    shelter = Shelter()
    animal = shelter.add_animal(name='Matilda', year_of_birth=2012, gender='F', date_of_entry=date(2016, 1, 27),
                                species='dog', breed='Chow chow')
    test_json_sql(shelter)
    shelter.add_foster_parent("Jury", "New York", "02355975", 2)
    test_json_sql(shelter)
    store(shelter, db=left)

    shelter_json_1_animal = shelter_json.store(shelter.animals)
    shelter_json_1_fosters = shelter_json.store(shelter.fosters)

    assert len(json.loads(shelter_json_1_animal)) == len(shelter.animals)
    assert len(json.loads(shelter_json_1_fosters)) == len(shelter.fosters)
    animal.adopt(date(2016, 2, 27), "Adam", "Paris")
    shelter_json_back = shelter_json.load(shelter_json_1_animal, shelter_json_1_fosters)
    shelter_json_back_i = store(shelter_json_back, db=left, deduplicate=True)

    assert len(shelter_json_back.animals) == len(shelter.animals)
    assert len(shelter_json_back.fosters) == len(shelter.fosters)

    ident_1_l = store(shelter, db=left)
    ident_1_r = store(shelter, db=right)
    ident_2_l = store(shelter, db=left, deduplicate=True)
    assert ident_1_l == ident_2_l


def test_basic_leftovers2():
    left = unlink_and_make_db("shelter.dat", True)
    right = unlink_and_make_db("shelter.dat", False)

    shelter = Shelter()
    animal = shelter.add_animal(name='Matilda', year_of_birth=2012, gender='F', date_of_entry=date(2016, 1, 27),
                                species='dog', breed='Chow chow')
    test_json_sql(shelter)
    shelter.add_foster_parent("Jury", "New York", "02355975", 2)
    test_json_sql(shelter)
    animal.adopt(date(2016, 2, 27), "Adam", "Paris")
    test_json_sql(shelter)
    store(shelter, db=left)

    animal.adopter = None

    ident_1_l = store(shelter, db=left)
    ident_1_r = store(shelter, db=right)
    ident_2_l = store(shelter, db=left, deduplicate=True)
    assert ident_1_l == ident_2_l


def test_basic_leftovers3():
    left = unlink_and_make_db("shelter.dat", True)
    right = unlink_and_make_db("shelter.dat", False)

    shelter = Shelter()
    animal = shelter.add_animal(name='Matilda', year_of_birth=2012, gender='F', date_of_entry=date(2016, 1, 27),
                                species='dog', breed='Chow chow')

    animal.adopt(date(2016, 2, 27), "Adam", "Paris")
    store(shelter, db=left)

    shelter.add_foster_parent("Jury", "New York", "02355975", 2)

    ident_1_l = store(shelter, db=left)
    ident_1_r = store(shelter, db=right)
    ident_2_l = store(shelter, db=left, deduplicate=True)
    assert ident_1_l == ident_2_l
    test_json_sql(shelter)


def test_basic_leftovers4():
    left = unlink_and_make_db("shelter.dat", True)
    right = unlink_and_make_db("shelter.dat", False)

    shelter = Shelter()

    shelter.add_foster_parent("Jury", "New York", "02355975", 2)
    store(shelter, db=left)

    animal = shelter.add_animal(name='Matilda', year_of_birth=2012, gender='F', date_of_entry=date(2016, 1, 27),
                                species='dog', breed='Chow chow')

    ident_1_l = store(shelter, db=left)
    ident_1_r = store(shelter, db=right)
    ident_2_l = store(shelter, db=left, deduplicate=True)
    assert ident_1_l == ident_2_l
    test_json_sql(shelter)


def test_basic_leftovers5():
    left = unlink_and_make_db("shelter.dat", True)
    right = unlink_and_make_db("shelter.dat", False)

    shelter = Shelter()
    animal = shelter.add_animal(name='Matilda', year_of_birth=2012, gender='F', date_of_entry=date(2016, 1, 27),
                                species='dog', breed='Chow chow')
    shelter.add_foster_parent("Jury", "New York", "02355975", 2)
    animal.adopt(date(2016, 4, 27), "Adam", "Paris")
    store(shelter, db=left)
    test_json_sql(shelter)
    animal.add_exam("Pro Care", date(2016, 2, 27), "Preparing for death.")

    ident_1_l = store(shelter, db=left)
    ident_1_r = store(shelter, db=right)
    ident_2_l = store(shelter, db=left, deduplicate=True)
    assert ident_1_l == ident_2_l
    test_json_sql(shelter)


def test_basic_leftovers6():
    left = unlink_and_make_db("shelter.dat", True)
    right = unlink_and_make_db("shelter.dat", False)

    shelter = Shelter()
    animal = shelter.add_animal(name='Matilda', year_of_birth=2012, gender='F', date_of_entry=date(2016, 1, 27),
                                species='dog', breed='Chow chow')
    shelter.add_foster_parent("Jury", "New York", "02355975", 2)
    animal.adopt(date(2016, 4, 27), "Adam", "Paris")
    animal.add_exam("Pro Care", date(2016, 2, 27), "Preparing for death.")
    store(shelter, db=left)

    animal.veterinary_records = None

    ident_1_l = store(shelter, db=left)
    ident_1_r = store(shelter, db=right)
    ident_2_l = store(shelter, db=left, deduplicate=True)
    assert ident_1_l == ident_2_l
    test_json_sql(shelter)


def test_basic_leftovers7():
    left = unlink_and_make_db("shelter.dat", True)
    right = unlink_and_make_db("shelter.dat", False)

    shelter = Shelter()
    animal = shelter.add_animal(name='Matilda', year_of_birth=2012, gender='F', date_of_entry=date(2016, 1, 27),
                                species='dog', breed='Chow chow')
    shelter.add_foster_parent("Jury", "New York", "02355975", 2)
    store(shelter, db=left)

    animal.start_foster(date(2016, 2, 27), shelter.fosters[0])
    test_json_sql(shelter)
    ident_1_l = store(shelter, db=left)
    ident_1_r = store(shelter, db=right)
    ident_2_l = store(shelter, db=left, deduplicate=True)
    assert ident_1_l == ident_2_l
    test_json_sql(shelter)


def test_basic_leftovers8():
    left = unlink_and_make_db("shelter.dat", True)
    right = unlink_and_make_db("shelter.dat", False)

    shelter = Shelter()
    animal = shelter.add_animal(name='Matilda', year_of_birth=2012, gender='F', date_of_entry=date(2016, 1, 27),
                                species='dog', breed='Chow chow')
    shelter.add_foster_parent("Jury", "New York", "02355975", 2)
    animal.start_foster(date(2016, 2, 27), shelter.fosters[0])
    store(shelter, db=left)

    animal.end_foster(date(2016, 3, 27))

    ident_1_l = store(shelter, db=left)
    ident_1_r = store(shelter, db=right)
    ident_2_l = store(shelter, db=left, deduplicate=True)
    assert ident_1_l == ident_2_l
    test_json_sql(shelter)


def test_main_sql():
    db = unlink_and_make_db("shelter.dat", False)
    test_store_shelter(db)


def test_massive_brut_force(amount):
    iteration = 0
    for _ in range(0, amount):
        iteration += 1
        try:
            print("Starting iteration " + str(iteration) + ":")
            print(" BASIC:")
            test_fail2()
            print(" ADVANCED:")
            test_fail1()
            print(" PASS\n")
        except AssertionError:
            print(" FAIL\n")
            raise RuntimeError("Iteration" + str(iteration) + ": FAIL")


def test_json_sql(shelter, printing=False):
    conn = unlink_and_make_db("shelter_json.dat", True)
    c_fake = unlink_and_make_db("shelter_json.dat", False)
    ident = store(shelter, db=conn)
    shelter_2 = load(ident, db=conn)
    store(shelter, db=c_fake)
    shelter_i = store(shelter_2, db=conn, deduplicate=True)

    assert shelter_i == ident

    shelter_json_1_animal = shelter_json.store(shelter.animals)
    shelter_json_1_fosters = shelter_json.store(shelter.fosters)

    assert len(json.loads(shelter_json_1_animal)) == len(shelter.animals)
    assert len(json.loads(shelter_json_1_fosters)) == len(shelter.fosters)

    shelter_json_back = shelter_json.load(shelter_json_1_animal, shelter_json_1_fosters)
    shelter_json_back_i = store(shelter_json_back, db=conn, deduplicate=True)

    assert len(shelter_json_back.animals) == len(shelter.animals)
    assert len(shelter_json_back.fosters) == len(shelter.fosters)
    assert shelter_json_back_i == ident

    if printing:
        print("  Shelter Json: PASS")


def test_failX():
    conn = unlink_and_make_db("shelter_json_fail.dat", True)
    when = datetime.date(2016, 9, 23)
    s = Shelter()
    brute = s.add_animal(name='Brute', year_of_birth=2014, gender='M', date_of_entry=datetime.date(2016, 9, 23),
                         species='platypus', breed='unknown')
    brute.add_exam('Mark Care', datetime.date(2016, 11, 24), 'Routine entrance exam.')
    brute.add_exam('Pro Care', datetime.date(2017, 1, 3), 'Bald spot in fur examined.')

    rex = s.add_animal(name='Rex', year_of_birth=2020, gender='M', date_of_entry=datetime.date(2020, 7, 7),
                       species='dog', breed='Shepherd')
    rex.add_exam('Antonin Care', datetime.date(2020, 9, 7), 'Thorough entrance exam.')

    s.add_foster_parent('Freddy', 'New York', '09542985248', 3)

    brute.start_foster(when, s.available_foster_parents(when)[0])

    ident = store(s, db=conn)
    s2 = load(ident, db=conn)

    fosters = shelter_json.store(s2.available_foster_parents(when))
    dict_form = json.loads(fosters)
    assert len(dict_form[0]["fostering"]) == 1


if __name__ == '__main__':
    test_failX()
    test_basic_empty()
    test_basic1()
    test_main_shelter()
    test_main_sql()
    test_sanity_fail1()
    test_basic_leftovers1()
    test_basic_leftovers2()
    test_basic_leftovers3()
    test_basic_leftovers4()
    test_basic_leftovers5()
    test_basic_leftovers6()
    test_basic_leftovers7()
    test_basic_leftovers8()
    test_massive_brut_force(3)  # Can be higher
