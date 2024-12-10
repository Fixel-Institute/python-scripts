#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  6 08:58:47 2022

@author: Jackson Cagle
"""

import numpy as np
import struct

def decodeAppleWatchStructureRaw(rawBytes):
    currentIndex = 72
    Headers = rawBytes[:currentIndex].decode("utf-8").rstrip("\x00")

    Data = {"DeviceID": Headers, "Accelerometer": {"Time": [], "Data": []}, "Gyroscope": {"Time": [], "Data": []}, 
            "TremorSeverity": {"Time": [], "TimeRange": [], "Data": []}, 
            "DyskineticProbability": {"Time": [], "TimeRange": [], "Data": []}, 
            "HeartRate": {"Time": [], "TimeRange": [], "Data": [], "MotionContext": []}, 
            "HeartRateVariability": {"Time": [], "TimeRange": [], "Data": []}, 
            "SleepState": {"Time": [], "TimeRange": [], "Data": []}}
    
    referenceTimestamp = np.frombuffer(rawBytes[currentIndex:currentIndex+8], np.float64, count=1)[0]
    currentIndex += 8

    while currentIndex < len(rawBytes)-1:
        DataType = rawBytes[currentIndex]
        if DataType == 79:
            DataValues = np.frombuffer(rawBytes[currentIndex+2:currentIndex+8], np.int16, count=3) / 1000
            Timestamp = np.frombuffer(rawBytes[currentIndex+8:currentIndex+16], np.float64, count=1)[0] + referenceTimestamp
            Data["Gyroscope"]["Time"].append(Timestamp)
            Data["Gyroscope"]["Data"].append(DataValues)
            currentIndex += 16
        elif DataType == 80:
            DataValues = np.frombuffer(rawBytes[currentIndex+2:currentIndex+8], np.int16, count=3) / 1000
            Timestamp = np.frombuffer(rawBytes[currentIndex+8:currentIndex+16], np.float64, count=1)[0] + referenceTimestamp
            Data["Accelerometer"]["Time"].append(Timestamp)
            Data["Accelerometer"]["Data"].append(DataValues)
            currentIndex += 16
        elif DataType == 81:
            DataValues = np.frombuffer(rawBytes[currentIndex+1:currentIndex+7], np.int8, count=6) / 100
            TimeRange = np.frombuffer(rawBytes[currentIndex+8:currentIndex+10], np.uint16, count=1)[0]
            Timestamp = np.frombuffer(rawBytes[currentIndex+12:currentIndex+16], np.float32, count=1)[0] + referenceTimestamp
            Data["TremorSeverity"]["Time"].append(Timestamp)
            Data["TremorSeverity"]["TimeRange"].append(TimeRange)
            Data["TremorSeverity"]["Data"].append(DataValues)
            currentIndex += 16
        elif DataType == 82:
            DataValues = rawBytes[currentIndex+1]/255
            TimeRange = np.frombuffer(rawBytes[currentIndex+2:currentIndex+4], np.uint16, count=1)[0]
            Timestamp = np.frombuffer(rawBytes[currentIndex+4:currentIndex+8], np.float32, count=1)[0] + referenceTimestamp
            Data["DyskineticProbability"]["Time"].append(Timestamp)
            Data["DyskineticProbability"]["TimeRange"].append(TimeRange)
            Data["DyskineticProbability"]["Data"].append(DataValues)
            currentIndex += 8
        elif DataType == 128:
            DataValues = np.frombuffer(rawBytes[currentIndex+2:currentIndex+4], np.uint16, count=1)[0]
            MotionContext = rawBytes[currentIndex+1]
            TimeRange = np.frombuffer(rawBytes[currentIndex+4:currentIndex+6], np.uint16, count=1)[0]
            Timestamp = np.frombuffer(rawBytes[currentIndex+8:currentIndex+16], np.float64, count=1)[0]
            Data["HeartRate"]["Time"].append(Timestamp)
            Data["HeartRate"]["TimeRange"].append(TimeRange)
            Data["HeartRate"]["Data"].append(DataValues)
            Data["HeartRate"]["MotionContext"].append(MotionContext)
            currentIndex += 16
        elif DataType == 129:
            DataValues = np.frombuffer(rawBytes[currentIndex+2:currentIndex+4], np.uint16, count=1)[0]
            TimeRange = np.frombuffer(rawBytes[currentIndex+4:currentIndex+6], np.uint16, count=1)[0]
            Timestamp = np.frombuffer(rawBytes[currentIndex+8:currentIndex+16], np.float64, count=1)[0]
            Data["HeartRateVariability"]["Time"].append(Timestamp)
            Data["HeartRateVariability"]["TimeRange"].append(TimeRange)
            Data["HeartRateVariability"]["Data"].append(DataValues)
            currentIndex += 16
        elif DataType == 130:
            DataValues = rawBytes[currentIndex+1]
            TimeRange = np.frombuffer(rawBytes[currentIndex+2:currentIndex+4], np.uint16, count=1)[0]
            Timestamp = np.frombuffer(rawBytes[currentIndex+8:currentIndex+16], np.float64, count=1)[0]
            Data["SleepState"]["Time"].append(Timestamp)
            Data["SleepState"]["TimeRange"].append(TimeRange)
            Data["SleepState"]["Data"].append(DataValues)
            currentIndex += 16
        else:
            print("New Data")
            raise Exception
                
    Data["Accelerometer"]["Time"] = np.array(Data["Accelerometer"]["Time"])
    Data["Accelerometer"]["Data"] = np.array(Data["Accelerometer"]["Data"])
    Data["DyskineticProbability"]["Time"] = np.array(Data["DyskineticProbability"]["Time"])
    Data["DyskineticProbability"]["TimeRange"] = np.array(Data["DyskineticProbability"]["TimeRange"])
    Data["DyskineticProbability"]["Data"] = np.array(Data["DyskineticProbability"]["Data"])
    Data["TremorSeverity"]["Time"] = np.array(Data["TremorSeverity"]["Time"])
    Data["TremorSeverity"]["TimeRange"] = np.array(Data["TremorSeverity"]["TimeRange"])
    Data["TremorSeverity"]["Data"] = np.array(Data["TremorSeverity"]["Data"])
    Data["HeartRate"]["Time"] = np.array(Data["HeartRate"]["Time"])
    Data["HeartRate"]["TimeRange"] = np.array(Data["HeartRate"]["TimeRange"])
    Data["HeartRate"]["Data"] = np.array(Data["HeartRate"]["Data"])
    Data["HeartRateVariability"]["Time"] = np.array(Data["HeartRateVariability"]["Time"])
    Data["HeartRateVariability"]["TimeRange"] = np.array(Data["HeartRateVariability"]["TimeRange"])
    Data["HeartRateVariability"]["Data"] = np.array(Data["HeartRateVariability"]["Data"])
    Data["SleepState"]["Time"] = np.array(Data["SleepState"]["Time"])
    Data["SleepState"]["TimeRange"] = np.array(Data["SleepState"]["TimeRange"])
    Data["SleepState"]["Data"] = np.array(Data["SleepState"]["Data"])

    return Data

def decodeAppleWatchStructure(filenames):
    if type(filenames) == str:
        listOfFiles = [filenames]
    else:
        listOfFiles = filenames
    
    Data = {"DeviceID": "", "Accelerometer": {"Time": [], "Data": []}, 
            "TremorSeverity": {"Time": [], "TimeRange": [], "Data": []}, 
            "DyskineticProbability": {"Time": [], "TimeRange": [], "Data": []}, 
            "HeartRate": {"Time": [], "TimeRange": [], "Data": [], "MotionContext": []}, 
            "HeartRateVariability": {"Time": [], "TimeRange": [], "Data": []}, 
            "SleepState": {"Time": [], "TimeRange": [], "Data": []}}
        
    for filename in listOfFiles:
        with open(filename, "rb") as fid:
            rawBytes = fid.read()
        
        currentIndex = 72
        Headers = rawBytes[:currentIndex].decode("utf-8").rstrip("\x00")
        Data["DeviceID"] = Headers
        
        referenceTimestamp = np.frombuffer(rawBytes[currentIndex:currentIndex+8], np.float64, count=1)[0]
        currentIndex += 8

        while currentIndex < len(rawBytes)-1:
            DataType = rawBytes[currentIndex]
            if DataType == 79:
                DataValues = np.frombuffer(rawBytes[currentIndex+2:currentIndex+8], np.int16, count=3) / 1000
                Timestamp = np.frombuffer(rawBytes[currentIndex+8:currentIndex+16], np.float64, count=1)[0] + referenceTimestamp
                Data["Accelerometer"]["Time"].append(Timestamp)
                Data["Accelerometer"]["Data"].append(DataValues)
                currentIndex += 16
            elif DataType == 80:
                DataValues = np.frombuffer(rawBytes[currentIndex+2:currentIndex+8], np.int16, count=3) / 1000
                Timestamp = np.frombuffer(rawBytes[currentIndex+8:currentIndex+12], np.float32, count=1)[0] + referenceTimestamp
                Data["Accelerometer"]["Time"].append(Timestamp)
                Data["Accelerometer"]["Data"].append(DataValues)
                currentIndex += 12
            elif DataType == 81:
                DataValues = np.frombuffer(rawBytes[currentIndex+1:currentIndex+7], np.int8, count=6) / 100
                TimeRange = np.frombuffer(rawBytes[currentIndex+8:currentIndex+10], np.uint16, count=1)[0]
                Timestamp = np.frombuffer(rawBytes[currentIndex+12:currentIndex+16], np.float32, count=1)[0] + referenceTimestamp
                Data["TremorSeverity"]["Time"].append(Timestamp)
                Data["TremorSeverity"]["TimeRange"].append(TimeRange)
                Data["TremorSeverity"]["Data"].append(DataValues)
                currentIndex += 16
            elif DataType == 82:
                DataValues = rawBytes[currentIndex+1]/255
                TimeRange = np.frombuffer(rawBytes[currentIndex+2:currentIndex+4], np.uint16, count=1)[0]
                Timestamp = np.frombuffer(rawBytes[currentIndex+4:currentIndex+8], np.float32, count=1)[0] + referenceTimestamp
                Data["DyskineticProbability"]["Time"].append(Timestamp)
                Data["DyskineticProbability"]["TimeRange"].append(TimeRange)
                Data["DyskineticProbability"]["Data"].append(DataValues)
                currentIndex += 8
            elif DataType == 128:
                DataValues = np.frombuffer(rawBytes[currentIndex+2:currentIndex+4], np.uint16, count=1)[0]
                MotionContext = rawBytes[currentIndex+1]
                TimeRange = np.frombuffer(rawBytes[currentIndex+4:currentIndex+6], np.uint16, count=1)[0]
                Timestamp = np.frombuffer(rawBytes[currentIndex+8:currentIndex+16], np.float64, count=1)[0]
                Data["HeartRate"]["Time"].append(Timestamp)
                Data["HeartRate"]["TimeRange"].append(TimeRange)
                Data["HeartRate"]["Data"].append(DataValues)
                Data["HeartRate"]["MotionContext"].append(MotionContext)
                currentIndex += 16
            elif DataType == 129:
                DataValues = np.frombuffer(rawBytes[currentIndex+2:currentIndex+4], np.uint16, count=1)[0]
                TimeRange = np.frombuffer(rawBytes[currentIndex+4:currentIndex+6], np.uint16, count=1)[0]
                Timestamp = np.frombuffer(rawBytes[currentIndex+8:currentIndex+16], np.float64, count=1)[0]
                Data["HeartRateVariability"]["Time"].append(Timestamp)
                Data["HeartRateVariability"]["TimeRange"].append(TimeRange)
                Data["HeartRateVariability"]["Data"].append(DataValues)
                currentIndex += 16
            elif DataType == 130:
                DataValues = rawBytes[currentIndex+1]
                TimeRange = np.frombuffer(rawBytes[currentIndex+2:currentIndex+4], np.uint16, count=1)[0]
                Timestamp = np.frombuffer(rawBytes[currentIndex+8:currentIndex+16], np.float64, count=1)[0]
                Data["SleepState"]["Time"].append(Timestamp)
                Data["SleepState"]["TimeRange"].append(TimeRange)
                Data["SleepState"]["Data"].append(DataValues)
                currentIndex += 16
            else:
                print("New Data")
                raise Exception
                
    Data["Accelerometer"]["Time"] = np.array(Data["Accelerometer"]["Time"])
    Data["Accelerometer"]["Data"] = np.array(Data["Accelerometer"]["Data"]) / 1000
    Data["DyskineticProbability"]["Time"] = np.array(Data["DyskineticProbability"]["Time"])
    Data["DyskineticProbability"]["Data"] = np.array(Data["DyskineticProbability"]["Data"])
    Data["TremorSeverity"]["Time"] = np.array(Data["TremorSeverity"]["Time"])
    Data["TremorSeverity"]["Data"] = np.array(Data["TremorSeverity"]["Data"]) / 100
    Data["HeartRate"]["Time"] = np.array(Data["HeartRate"]["Time"])
    Data["HeartRate"]["Data"] = np.array(Data["HeartRate"]["Data"])
    Data["SleepState"]["Time"] = np.array(Data["SleepState"]["Time"])
    Data["SleepState"]["Data"] = np.array(Data["SleepState"]["Data"])
    
    return Data

def decodeMetaMotionStructureRaw(rawBytes):
    currentIndex = 80
    Headers = rawBytes[:currentIndex].decode("utf-8").rstrip("\x00")
    
    Data = {"DeviceID": Headers, "Accelerometer": {"Time": [], "Data": []}, "RSSAccelerometer": {"Time": [], "Data": []}, "Gyroscope": {"Time": [], "Data": []}, "AmbientLight": {"Time": [], "Data": []}}
    while currentIndex < len(rawBytes)-1:
        DataType = rawBytes[currentIndex]
        if DataType == 25:
            DataValues = np.frombuffer(rawBytes[currentIndex+4:currentIndex+16], np.float32, count=3)
            Timestamp = np.frombuffer(rawBytes[currentIndex+16:currentIndex+24], np.float64, count=1)[0]
            Data["Accelerometer"]["Time"].append(Timestamp)
            Data["Accelerometer"]["Data"].append(DataValues)
            currentIndex += 24
        elif DataType == 26:
            DataValues = np.frombuffer(rawBytes[currentIndex+4:currentIndex+8], np.float32, count=1)[0]
            Timestamp = np.frombuffer(rawBytes[currentIndex+8:currentIndex+16], np.float64, count=1)[0]
            Data["RSSAccelerometer"]["Time"].append(Timestamp)
            Data["RSSAccelerometer"]["Data"].append(DataValues)
            currentIndex += 16
        elif DataType == 35:
            DataValues = np.frombuffer(rawBytes[currentIndex+4:currentIndex+16], np.float32, count=3)
            Timestamp = np.frombuffer(rawBytes[currentIndex+16:currentIndex+24], np.float64, count=1)[0]
            Data["Gyroscope"]["Time"].append(Timestamp)
            Data["Gyroscope"]["Data"].append(DataValues)
            currentIndex += 24
        elif DataType == 45:
            DataValues = np.frombuffer(rawBytes[currentIndex+4:currentIndex+8], np.uint32, count=1)[0]
            Timestamp = np.frombuffer(rawBytes[currentIndex+8:currentIndex+16], np.float64, count=1)[0]
            Data["AmbientLight"]["Time"].append(Timestamp)
            Data["AmbientLight"]["Data"].append(DataValues)
            currentIndex += 16
    
    Data["Accelerometer"]["Time"] = np.array(Data["Accelerometer"]["Time"])
    Data["Accelerometer"]["Data"] = np.array(Data["Accelerometer"]["Data"])
    Data["RSSAccelerometer"]["Time"] = np.array(Data["RSSAccelerometer"]["Time"])
    Data["RSSAccelerometer"]["Data"] = np.array(Data["RSSAccelerometer"]["Data"])
    Data["Gyroscope"]["Time"] = np.array(Data["Gyroscope"]["Time"])
    Data["Gyroscope"]["Data"] = np.array(Data["Gyroscope"]["Data"])
    Data["AmbientLight"]["Time"] = np.array(Data["AmbientLight"]["Time"])
    Data["AmbientLight"]["Data"] = np.array(Data["AmbientLight"]["Data"])
    
    return Data

def decodeBRAVOWearableStructure(filename):
    with open(filename, "rb") as file:
        rawBytes = file.read()

    Data = decodeMetaMotionStructureRaw(rawBytes)
    return Data