
import requests
import json
import os
import pickle as pkl
import datetime

class BRAVOPlatformRequest:
    def __init__(self, api_key, server="http://localhost"):
        self.__Server = server
        self.__request = requests.Session()
        self.__API_Key = api_key
    
        self.__ActiveStudy = None
        self.__ActiveParticipant = None
        self.__ActiveEvent = None

        self.DatabaseOverview = self.QueryStudyParticipants()
        self.ParticipantEvents = {}

    def query(self, url, data=None, files=None, content_type="application/json"):
        if not content_type:
            Headers = {"X-Secure-API-Key": self.__API_Key}
        else:
            Headers = {"Content-Type": content_type, "X-Secure-API-Key": self.__API_Key}

        if data:
            return self.__request.post(self.__Server + url,
                                       json.dumps(data) if content_type else data,
                                       headers=Headers)
        elif files:
            return self.__request.post(self.__Server + url,
                                       files=files,
                                       headers=Headers)
        else:
            return self.__request.post(self.__Server + url,
                                       headers=Headers)
    
    def QueryStudyParticipants(self):
        response = self.query("/api/queryStudyParticipant")
        if response.status_code == 200:
            payload = response.json()
            self.DatabaseOverview = payload
            return payload
        else:
            if response.status_code == 400:
                raise Exception(f"Network Error: {response.json()}")
            else:
                raise Exception(f"Network Error: {response.status_code}")
    
    def FindStudyParticipant(self, study_name, participant_name):
        for study in self.DatabaseOverview["studies"]:
            if study["name"] == study_name:
                for participant in self.DatabaseOverview["participants"][study["uid"]]:
                    if participant["name"] == participant_name:
                        participant["study"] = study["uid"]
                        return participant

    def SetActiveParticipant(self, study_name, participant_name):
        participant = self.FindStudyParticipant(study_name, participant_name)
        if not participant:
            return None
        self.__ActiveParticipant = participant
        self.__ActiveEvent = None
        return self.__ActiveParticipant
    
    def GetParticipantInfo(self, participant=None):
        if not participant:
            response = self.query("/api/queryParticipantInformation", { "participant_uid": self.__ActiveParticipant["uid"] })
        else:
            response = self.query("/api/queryParticipantInformation", { "participant_uid": participant["uid"] })
        
        if response.status_code == 200:
            payload = response.json()
            return payload
        else:
            if response.status_code == 400:
                raise Exception(f"Network Error: {response.json()}")
            else:
                raise Exception(f"Network Error: {response.status_code}")

    def CreateStudyParticipant(self, study_name, participant_name, dob=None, sex=None, diagnosis=None, disease_start_time=None):
        data = {"name": participant_name, "study": study_name, "dob": dob, "sex": sex, 
                "diagnosis": diagnosis, "disease_start_time": disease_start_time}
        
        response = self.query("/api/createStudyParticipant", data)
        if response.status_code == 200:
            payload = response.json()
            
            NewStudy = None
            for study in self.DatabaseOverview["studies"]:
                if study["name"] == study_name:
                    NewStudy = study
                    break
            if not NewStudy:
                self.DatabaseOverview["studies"].append({
                    "uid": payload["study"],
                    "name": study_name
                })
                self.DatabaseOverview["participants"][payload["study"]] = []
            self.DatabaseOverview["participants"][payload["study"]].append(payload)
            self.__ActiveParticipant = payload
            self.__ActiveEvent = None
            return payload
        else:
            if response.status_code == 400:
                raise Exception(f"Network Error: {response.json()}")
            else:
                raise Exception(f"Network Error: {response.status_code}")
    
    def DeleteStudyParticipant(self, study_name, participant_name):
        data = None
        for study in self.DatabaseOverview["studies"]:
            if study["name"] == study_name:
                for participant in self.DatabaseOverview["participants"][study["uid"]]:
                    if participant["name"] == participant_name:
                        data = {"name": participant["uid"], "study": study["uid"]}
        
        if not data:
            raise Exception("Study Participant not found. Run QueryStudyParticipants to Synchronize Database.")
        
        response = self.query("/api/deleteStudyParticipant", data)
        if response.status_code == 200:
            payload = response.json()
            self.DatabaseOverview["participants"][data["study"]] = [participant for participant in self.DatabaseOverview["participants"][data["study"]] if not participant["uid"] == data["name"]]
            self.__ActiveParticipant = None
            self.__ActiveEvent = None
            return payload
        else:
            if response.status_code == 400:
                raise Exception(f"Network Error: {response.json()}")
            else:
                raise Exception(f"Network Error: {response.status_code}")
    
    def CreateParticipantEvent(self, event_type, event_name, participant=None, date=None):
        if not participant:
            data = {"participant_uid": self.__ActiveParticipant["uid"], "study": self.__ActiveParticipant["study"], "event_name": event_name, "event_type": event_type}
            study_id = self.__ActiveParticipant["study"]
            if not "events" in self.__ActiveParticipant.keys():
                self.QueryParticipantEvent()
        else:
            data = {"participant_uid": participant["uid"], "study": participant["study"], "event_name": event_name, "event_type": event_type}
            study_id = participant["study"]
            if not "events" in participant.keys():
                self.QueryParticipantEvent(participant)

        if date:
            data["date"] = date
        
        response = self.query("/api/createParticipantEvent", data)
        if response.status_code == 200:
            payload = response.json()
            for participantObj in self.DatabaseOverview["participants"][study_id]:
                if participantObj["uid"] == data["participant_uid"]:
                    if not event_type in participantObj["events"].keys():
                        participantObj["events"][event_type] = []
                    participantObj["events"][event_type].append(payload)
                    break
            self.__ActiveEvent = payload
            return payload
        else:
            if response.status_code == 400:
                raise Exception(f"Network Error: {response.json()}")
            else:
                raise Exception(f"Network Error: {response.status_code}")
    
    def QueryParticipantEvent(self, participant=None):
        if not participant:
            data = {"participant_uid": self.__ActiveParticipant["uid"]}
            study_id = self.__ActiveParticipant["study"]
        else:
            data = {"participant_uid": participant["uid"]}
            study_id = participant["study"]
        
        response = self.query("/api/queryParticipantEvents", data)
        if response.status_code == 200:
            payload = response.json()
            for participantObj in self.DatabaseOverview["participants"][study_id]:
                if participantObj["uid"] == data["participant_uid"]:
                    participantObj["events"] = payload
                    break
            return payload
        else:
            if response.status_code == 400:
                raise Exception(f"Network Error: {response.json()}")
            else:
                raise Exception(f"Network Error: {response.status_code}")
    
    def UploadData(self, data_type, files, metadata, event_type, event_name, participant=None, study=None):
        Participant = None
        if participant:
            if study:
                Participant = self.FindStudyParticipant(study, participant)
            else:
                Participant = participant
        else:
            Participant = self.__ActiveParticipant
        
        form = {"study": (None, Participant["study"]), 
                "participant": (None, Participant["uid"]), 
                "data_type": (None, data_type), 
                "metadata": (None, json.dumps(metadata))}
        
        if type(files) == list:
            for i in range(len(files)):
                form["file" + str(i)] = files[i]
        else:
            form["file"] = files

        if not event_type in Participant["events"].keys():
            print("Event not found. Creating new event")
            event = self.CreateParticipantEvent(event_name, event_type, participant=Participant)
            form["event"] = event["uid"]
        else:
            for event in Participant["events"][event_type]:
                if event["name"] == event_name:
                    form["event"] = (None, event["uid"])
                    break 
        
        response = self.query("/api/uploadData", files=form, content_type=None)
        if response.status_code == 200:
            payload = response.json()
            return payload
        else:
            if response.status_code == 400:
                raise Exception(f"Network Error: {response.json()}")
            else:
                raise Exception(f"Network Error: {response.status_code}")
            
    def RetrieveDataList(self, participant=None, study=None, event_type=None, event_name=None):
        participantObj = self.__ActiveParticipant
        if not study:
            if participant:
                participantObj = participant
        else:
            if participant:
                participantObj = self.FindStudyParticipant(study, participant)
        data = {"participant": participantObj["uid"], "study": participantObj["study"]}

        if event_type:
            if not "events" in participantObj.keys():
                participantObj["events"] = self.QueryParticipantEvent(participant=participantObj)
            events = participantObj["events"][event_type]
        else:
            raise Exception("Event Type and Event Name must be provided")
        
        if not event_name:
            results = []
            for event in events:
                data["event"] = event["uid"]
                response = self.query("/api/retrieveDataList", data)
                if response.status_code == 200:
                    payload = response.json()
                    results.append(dict(event, **{"files": payload}))
                else:
                    if response.status_code == 400:
                        raise Exception(f"Network Error: {response.json()}")
                    else:
                        raise Exception(f"Network Error: {response.status_code}")
            return results
        
        else:
            for event in events:
                if event["name"] == event_name:
                    data["event"] = event["uid"]
                    response = self.query("/api/retrieveDataList", data)
                    if response.status_code == 200:
                        payload = response.json()
                        return dict(event, **{"files": payload})
                    else:
                        if response.status_code == 400:
                            raise Exception(f"Network Error: {response.json()}")
                        else:
                            raise Exception(f"Network Error: {response.status_code}")

        raise Exception("Event Name not found")
    
    def RetrieveData(self, recording_uid, participant=None, save_as=None):
        participantObj = participant if participant else self.__ActiveParticipant
        data = {"participant": participantObj["uid"], "study": participantObj["study"], "recording_uid": recording_uid}

        response = self.query("/api/retrieveData", data, content_type=None)
        if response.status_code == 200:
            payload = response.content
            if save_as:
                with open(save_as, "wb+") as file:
                    file.write(payload)
                return save_as
            return payload
        else:
            if response.status_code == 400:
                raise Exception(f"Network Error: {response.json()}")
            else:
                raise Exception(f"Network Error: {response.status_code}")

    def DeleteData(self, recording_uid, participant=None):
        participantObj = participant if participant else self.__ActiveParticipant
        data = {"participant": participantObj["uid"], "study": participantObj["study"], "recording_uid": recording_uid}

        response = self.query("/api/deleteData", data)
        if response.status_code == 200:
            return True
        else:
            if response.status_code == 400:
                raise Exception(f"Network Error: {response.json()}")
            else:
                raise Exception(f"Network Error: {response.status_code}")
