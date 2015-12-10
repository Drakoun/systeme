package fr.unice.miage.sd.tinydfs.tests.clients;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Properties;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import fr.unice.miage.sd.tinydfs.nodes.Master;
import fr.unice.miage.sd.tinydfs.tests.config.Constants;

public class WhatSizeTest {
	private static String storageServiceName;
	private static String registryHost; 
	private static Master master;
	
	@BeforeClass
	public static void setUp() {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream(ClientsTest.class.getResource(
					Constants.PROPERTIES_FILE_PATH).getFile());
			prop.load(input);
			storageServiceName = prop.getProperty(
					Constants.SERVICE_NAME_PROPERTY_KEY);
			registryHost = prop.getProperty(
					Constants.REGISTRY_HOST_PROPERTY_KEY);
		} 
		catch (IOException e) {
			e.printStackTrace();
		} 
		finally {
			if (input != null) {
				try {
					input.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		try {
			Registry registry = LocateRegistry.getRegistry(
					registryHost, Registry.REGISTRY_PORT);
			master = (Master) registry.lookup(storageServiceName);
		} 
		catch (RemoteException | NotBoundException e) {
			e.printStackTrace();
			System.err.println("[WhatSizeTest] No master found, exiting.");
			System.exit(1);
		}
		try {
			Thread.sleep(500);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void sizeBinarySampleTest() {
		
		try {
			File binarySample = new File(this.getClass().getResource(
					Constants.BINARY_SAMPLE_FILE_PATH).getFile());
			long expectedSizeB = binarySample.length();
			System.out.println(Constants.BINARY_SAMPLE_FILE_PATH);
			Assert.assertTrue(expectedSizeB == master.sizeOf(Constants.BINARY_SAMPLE_FILE_NAME));
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void sizeTextualSampleTest() {
		
		try {
			File textualSample = new File(this.getClass().getResource(
					Constants.TEXTUAL_SAMPLE_FILE_PATH).getFile());
			long expectedSizeT = textualSample.length();
			System.out.println(Constants.TEXTUAL_SAMPLE_FILE_PATH);
			Assert.assertTrue(expectedSizeT == master.sizeOf(Constants.TEXTUAL_SAMPLE_FILE_NAME));
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		
	}
	
}
