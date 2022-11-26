package com.example.DS2022_30441_Horvath_Ariana_2_MessageProducer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ConsumptionMessage {
    private String date;
    private String time;
    private float energyConsumption;
    private long meteringDeviceId;
}
