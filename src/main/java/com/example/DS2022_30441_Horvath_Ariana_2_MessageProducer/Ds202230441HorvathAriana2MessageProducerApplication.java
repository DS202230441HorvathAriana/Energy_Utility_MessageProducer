package com.example.DS2022_30441_Horvath_Ariana_2_MessageProducer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileNotFoundException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;

@SpringBootApplication
public class Ds202230441HorvathAriana2MessageProducerApplication {

	@Autowired
	MessageProducer messageProducer;
	private static String[] args;

	public static void main(String[] args) {
		Ds202230441HorvathAriana2MessageProducerApplication.args = args;
		SpringApplication.run(Ds202230441HorvathAriana2MessageProducerApplication.class, args);
	}

	@PostConstruct
	public void produceMessages() throws JsonProcessingException, FileNotFoundException, InterruptedException {
		long meteringDeviceId;
		if (args.length > 0)
			meteringDeviceId = Long.parseLong(args[0]);
		else
			throw new ArrayIndexOutOfBoundsException("No cmd arg given");

		Scanner sc = new Scanner(new File("sensor.csv"));
		sc.useDelimiter("\n");

		float energyConsumption = Float.parseFloat(sc.next());
		System.out.println(energyConsumption);
		ConsumptionMessage consumption = new ConsumptionMessage(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
				LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")),
				energyConsumption,
				meteringDeviceId);
		ObjectMapper mapper = new ObjectMapper();
		String jsonString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(consumption);
		messageProducer.publishMessage(jsonString);
		System.out.println("Message sent!!!");
		System.out.println(jsonString);
		while (sc.hasNext()) {
			Thread.sleep(5000);
			float newValue = Float.parseFloat(sc.next());
			float deltaConsumption = newValue - energyConsumption;
			energyConsumption = newValue;

			consumption = new ConsumptionMessage(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
					LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")),
					deltaConsumption,
					meteringDeviceId);
			jsonString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(consumption);

			messageProducer.publishMessage(jsonString);
			System.out.println("Message sent!!!");
			System.out.println(jsonString);
		}
	}
}
