import json

import shelter_sql
from shelter import *


def store(object_in, convert=False):
    if type(object_in) == Foster:
        return store_foster(object_in) if convert else json.dumps(store_foster(object_in))

    if type(object_in) == Animal:
        return store_animal(object_in) if convert else json.dumps(store_animal(object_in))

    if type(object_in) == list:
        out_list = []
        for object_value in object_in:
            out_list.append(store(object_value, True))
        return out_list if convert else json.dumps(out_list)
    raise RuntimeError("Function store can accept only Foster or Animal object or list of them.")


def store_foster(foster_object):
    foster_animals = []
    for animal in foster_object.animals_in_care:
        animal_info_dict = {"name": animal.name,
                            "year_of_birth": animal.year_of_birth,
                            "gender": animal.gender,
                            "date_of_entry": str(animal.date_of_entry),
                            "species": animal.species,
                            "breed": animal.breed}

        for record in animal.foster_records:
            if record.foster == foster_object:
                record_dict = {"start": str(record.period_from),
                               "end": record.period_to,
                               "animal": animal_info_dict}
                if record_dict["end"] is None:
                    del record_dict["end"]
                else:
                    record_dict["end"] = str(record_dict["end"])
                foster_animals.append(record_dict)

    foster_dict = {"name": foster_object.name,
                   "capacity": foster_object.max_animals,
                   "address": foster_object.address,
                   "phone": foster_object.phone_number,
                   "fostering": sorted(foster_animals, key=lambda x: x["start"])}
    return foster_dict


def store_animal(animal_object):
    adopt_dict = {}
    exams = []
    fostering = []
    if animal_object.adopter is not None:
        adopt_dict = {"date": str(animal_object.adopter[1]),
                      "name": animal_object.adopter[0].name,
                      "address": animal_object.adopter[0].address}

    if animal_object.veterinary_records is not None:
        for exam in animal_object.veterinary_records:
            exams.append({"vet": exam.vet,
                          "date": str(exam.date),
                          "report": exam.report})

    if animal_object.foster_records is not None:
        for foster_rec in animal_object.foster_records:
            output_dir = {"start": str(foster_rec.period_from),
                          "end": foster_rec.period_to,
                          "parent": {"name": foster_rec.foster.name,
                                     "address": foster_rec.foster.address,
                                     "phone": foster_rec.foster.phone_number}}
            if output_dir["end"] is None:
                del output_dir["end"]
            else:
                output_dir["end"] = str(output_dir["end"])
            fostering.append(output_dir)

    animal_dict = {"name": animal_object.name,
                   "year_of_birth": animal_object.year_of_birth,
                   "gender": animal_object.gender,
                   "date_of_entry": str(animal_object.date_of_entry),
                   "species": animal_object.species,
                   "breed": animal_object.breed,
                   "adopted": adopt_dict,
                   "exams": sorted(exams, key=lambda x: x["date"]),
                   "fostering": sorted(fostering, key=lambda x: x["start"])}

    if len(adopt_dict) == 0:
        del animal_dict["adopted"]
    return animal_dict


def convert_json_to_fosters(shelter, foster_json_data):
    for foster_data in foster_json_data:
        shelter.add_foster_parent(foster_data["name"], foster_data["address"], foster_data["phone"],
                                  foster_data["capacity"])


def convert_json_to_animal(shelter, animal_data, basic=False):
    animal = shelter.add_animal(animal_data["name"],
                                animal_data["year_of_birth"],
                                animal_data["gender"],
                                datetime.date.fromisoformat(animal_data["date_of_entry"]),
                                animal_data["species"],
                                animal_data["breed"])
    if basic:
        return animal

    if "adopted" in animal_data:
        animal.adopter = (Adopter(animal_data["adopted"]["name"], animal_data["adopted"]["address"]),
                          datetime.date.fromisoformat(animal_data["adopted"]["date"]))

    if len(animal_data["exams"]) > 0:
        out_exams = []
        for exam in animal_data["exams"]:
            out_exams.append(VetRecord(exam["vet"], datetime.date.fromisoformat(exam["date"]),
                                       exam["report"]))
        animal.veterinary_records = out_exams
    return animal


def load_full_shelter(shelter, json_animals, json_foster):
    foster_json_data = json.loads(json_foster)
    convert_json_to_fosters(shelter, foster_json_data)

    for animal_data in json.loads(json_animals):
        animal = convert_json_to_animal(shelter, animal_data)
        if len(animal_data["fostering"]) > 0:
            for animal_record in animal_data["fostering"]:
                for foster in shelter.fosters:
                    if foster.name == animal_record["parent"]["name"] \
                            and foster.address == animal_record["parent"]["address"] \
                            and foster.phone_number == animal_record["parent"]["phone"]:
                        parent = foster
                        break
                else:
                    raise RuntimeError("Cross-check failed, foster parent wasn't found.")

                for foster_data in foster_json_data:
                    found = False
                    if parent.name == foster_data["name"] and parent.address == foster_data["address"] \
                            and parent.phone_number == foster_data["phone"]:
                        for foster_record in foster_data["fostering"]:
                            if animal_record["start"] == foster_record["start"] and \
                                    (("end" not in animal_record and "end" not in foster_record)
                                     or animal_record["end"] == foster_record["end"]):
                                for animal_key in foster_record["animal"]:
                                    if foster_record["animal"][animal_key] != animal_data[animal_key]:
                                        break
                                else:
                                    foster_data["fostering"].remove(foster_record)
                                    found = True
                    if found:
                        break
                else:
                    raise RuntimeError("Cross-check failed, record about care wasn't found in foster parent records.")

                if "end" in animal_record:
                    animal.start_foster(datetime.date.fromisoformat(animal_record["start"]), parent, True,
                                        datetime.date.fromisoformat(animal_record["end"]))
                else:
                    animal.start_foster(datetime.date.fromisoformat(animal_record["start"]), parent, True)

    for foster_data in foster_json_data:
        if len(foster_data["fostering"]) > 0:
            raise RuntimeError("Cross-check failed, leftovers in fosters data.")
    return shelter


def get_foster_with_fake_animals(shelter, json_foster):
    foster_json_data = json.loads(json_foster)
    shelter.add_foster_parent(foster_json_data["name"], foster_json_data["address"], foster_json_data["phone"],
                              foster_json_data["capacity"])
    if len(shelter.fosters) != 1:
        raise RuntimeError("Invalid shelter structure.")

    for foster_record in foster_json_data["fostering"]:
        for existing_animal in shelter.animals:
            if existing_animal.name == foster_record["animal"]["name"] and \
                    existing_animal.year_of_birth == foster_record["animal"]["year_of_birth"] \
                    and existing_animal.gender == foster_record["animal"]["gender"] \
                    and existing_animal.date_of_entry == foster_record["animal"]["date_of_entry"] \
                    and existing_animal.species == foster_record["animal"]["species"] \
                    and existing_animal.breed == foster_record["animal"]["breed"]:
                fake_animal = existing_animal
                break
        else:
            fake_animal = convert_json_to_animal(shelter, foster_record["animal"], True)

        if "end" in foster_record:
            fake_animal.start_foster(datetime.date.fromisoformat(foster_record["start"]), shelter.fosters[0], True,
                                     datetime.date.fromisoformat(foster_record["end"]))
        else:
            fake_animal.start_foster(datetime.date.fromisoformat(foster_record["start"]), shelter.fosters[0], True)
    return shelter.fosters[0]


def get_animal_with_fake_fosters(shelter, json_animal):
    animal_data = json.loads(json_animal)
    animal = convert_json_to_animal(shelter, animal_data)
    for foster_record in animal_data["fostering"]:
        for foster in shelter.fosters:
            if foster.name == foster_record["parent"]["name"] and foster.address == foster_record["parent"]["address"] \
                    and foster.phone_number == foster_record["parent"]["phone"]:
                fake_foster = foster
                break
        else:
            fake_foster = Foster(foster_record["parent"]["name"],
                                 foster_record["parent"]["address"],
                                 foster_record["parent"]["phone"])
            shelter.fosters.append(fake_foster)

        if "end" in foster_record:
            animal.start_foster(datetime.date.fromisoformat(foster_record["start"]), fake_foster, True,
                                datetime.date.fromisoformat(foster_record["end"]))
        else:
            animal.start_foster(datetime.date.fromisoformat(foster_record["start"]), fake_foster, True)
    return animal


def load(first_param, second_param=None):
    shelter = Shelter()

    if type(first_param) != str or (second_param is not None and type(second_param) != str):
        raise RuntimeError("Arguments doesn't match data type requirements.")

    if second_param is not None:
        return load_full_shelter(shelter, first_param, second_param)

    object_dict = json.loads(first_param)
    if type(object_dict) != dict:
        raise RuntimeError("Invalid structure type.")

    if "species" in object_dict:
        return get_animal_with_fake_fosters(shelter, first_param)
    return get_foster_with_fake_animals(shelter, first_param)


def make_test_shelterX():
    shelter = Shelter()
    date1 = datetime.date(2003, 6, 1)
    shelter.add_animal('Adam', 1900, 'male', date1, 'dog', 'labrador')

    date2 = datetime.date(2001, 6, 1)
    shelter.add_animal('Johny', 2000, 'female', date2, 'rat', 'small')

    date3 = datetime.date(1996, 6, 2)
    shelter.add_animal('Gustav Big', 1980, 'female', date3, 'rat', 'big')

    shelter.add_foster_parent('Freddy', 'New York', '09542985248', 3)
    shelter.add_foster_parent('Pleb', 'Sydney', '0365848585', 1)
    shelter.add_foster_parent('Unknown', 'Moscow', '02254852928', 0)
    return date1, date2, date3, shelter


def test_json_load_and_store_animal():
    date1, date2, date3, shelter = make_test_shelterX()

    fosters = sorted(shelter.available_foster_parents(date1), key=lambda x: x.name)
    animals = shelter.list_animals(date3)
    assert len(animals) == 1

    animals[0].add_exam('Pro Care', datetime.date(2020, 9, 6), 'Thorough entrance exam.')
    animals[0].add_exam('Care Plus', datetime.date(2020, 8, 7), 'Suicide injection.')

    animals[0].start_foster(datetime.date(2000, 9, 6), fosters[0])
    animals[0].end_foster(datetime.date(2001, 9, 5))
    animals[0].start_foster(datetime.date(2001, 9, 6), fosters[0])
    animals[0].end_foster(datetime.date(2002, 9, 6))
    animals[0].start_foster(datetime.date(2004, 9, 6), fosters[0])
    animals[0].end_foster(datetime.date(2007, 9, 6))
    animals[0].adopt(datetime.date(2040, 9, 6), 'Patrick', 'Prague')

    original_json_fosters = store(shelter.fosters)
    original_json_animals = store(shelter.animals)
    new_shelter = load(original_json_animals, original_json_fosters)
    new_json_fosters = store(new_shelter.fosters)
    new_json_animals = store(new_shelter.animals)

    assert original_json_fosters == new_json_fosters and new_json_animals == original_json_animals

    for animal in shelter.animals:
        assert store(load(store(animal))) == store(animal)

    for foster in shelter.fosters:
        assert store(foster) == store(load(store(foster)))

    with open("shelter_json_test_redundant.json") as file:
        data = json.dumps(json.load(file))
        try:
            load(original_json_animals, data)
            raise AssertionError()
        except RuntimeError:
            pass


def test_failXY():
    s = Shelter()
    when = datetime.date(2030, 9, 23)
    brute = s.add_animal(name='Brute', year_of_birth=2014, gender='M', date_of_entry=datetime.date(2016, 9, 23),
                         species='platypus', breed='unknown')
    brute.add_exam('Mark Care', datetime.date(2016, 11, 24), 'Routine entrance exam.')
    brute.add_exam('Pro Care', datetime.date(2017, 1, 3), 'Bald spot in fur examined.')

    rex = s.add_animal(name='Rex', year_of_birth=2020, gender='M', date_of_entry=datetime.date(2020, 7, 7),
                       species='dog', breed='Shepherd')
    rex.add_exam('Antonin Care', datetime.date(2020, 9, 7), 'Thorough entrance exam.')

    s.add_foster_parent('Freddy', 'New York', '09542985248', 3)

    conn = shelter_sql.unlink_and_make_db("shelter_json_failXY.dat", True)

    brute.start_foster(datetime.date(2018, 9, 23), s.fosters[0])
    brute.end_foster(datetime.date(2030, 9, 24))

    id = shelter_sql.store(s, conn)
    s2 = shelter_sql.load(id, conn)

    for animal in s2.list_animals(date=when):
        pass

    animals = store(s2.animals)
    fosters = store(s2.fosters)
    shelter = load(animals, fosters)

    for animal in shelter.list_animals(date=when):
        pass


def test_main_json():
    test_json_load_and_store_animal()
    test_main_shelter()
    test_failXY()


if __name__ == '__main__':
    test_main_json()
