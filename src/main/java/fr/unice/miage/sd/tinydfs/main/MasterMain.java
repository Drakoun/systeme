package fr.unice.miage.sd.tinydfs.main;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import fr.unice.miage.sd.tinydfs.nodes.Master;
import fr.unice.miage.sd.tinydfs.nodes.Slave;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class MasterMain extends UnicastRemoteObject implements Master, Serializable {
    
    private String dfsRootFolder;
    private int nbSlaves;
    private Slave leftSlave;
    private Slave rightSlave;
    private List<String> listeEnregistrementEnCours = new ArrayList<String>();
    
    // Usage: java fr.unice.miage.sd.tinydfs.main.MasterMain storage_service_name dfs_root_folder nb_slaves
    public static void main(String[] args) throws RemoteException,
                AlreadyBoundException, NotBoundException, MalformedURLException, InterruptedException {
            String storageServiceName = args[0];
            String dfsRootFolder = args[1];
            int nbSlaves = Integer.parseInt(args[2]);

            // Create master and register it
            int rmiRegistryPort = 1099;
            Registry registry = LocateRegistry.createRegistry(rmiRegistryPort);
            System.out.println("Master : RMI registry listening on port " + rmiRegistryPort);
            
            MasterMain masterMain = new MasterMain(dfsRootFolder,nbSlaves);
            registry.bind(storageServiceName, masterMain);
            
            // On attend que tous les slaves se soient enregistrés avant de lancer la constuction de l'abre binaire
            attendreSlaves(nbSlaves);
            constructionArbreBinaireComplet(nbSlaves, masterMain);
    }

    private static void constructionArbreBinaireComplet(int nbSlaves, MasterMain masterMain) throws MalformedURLException, NotBoundException, RemoteException {
        System.out.println("Master : Tous les slaves sont créés. Création de l'arbre binaire complet");
        /* Verification que le nombre de Slaves permettent de créer un arbre binaire complet :
         * Un arbre binaire est complet lorsque le nombre de Slaves + 2 est égale à une puissance de 2 
         * Si c'est une puissance de 2, alors pour l'écriture en bytes de ce nombre il y a un seul bit 1 
         * puis que des bits 0, en excluant le cas nbSlaves = 1 */
        if (nbSlaves > 1 && (Long.bitCount(nbSlaves + 2) == 1)) {
            masterMain.setLeftSlave((Slave) Naming.lookup("rmi://localhost/" + "Slave2"));
            masterMain.setRightSlave((Slave) Naming.lookup("rmi://localhost/" + "Slave3"));
            System.out.println("Master : fils gauche et fils droit attribués");
            int i = 2;
            // A partir d'un noeud i, l'identifiant du fils gauche est 2*i et celui du droit 2*i+1
            while ((2 * i) <= nbSlaves) {
                Slave filsGauche = (Slave) Naming.lookup("rmi://localhost/" + "Slave" + (2*i));
                Slave filsDroit = (Slave) Naming.lookup("rmi://localhost/" + "Slave" + ((2*i) + 1));
                Slave noeud = (Slave) Naming.lookup("rmi://localhost/" + "Slave" + i);
                noeud.setLeftSlave(filsGauche);
                noeud.setRightSlave(filsDroit);
                i++;
            }
            System.out.println("Master : Création de l'arbe binaire complet réussi.");
        } else {
            System.out.println("Master : Création de l'arbre binaire complet échoué. Le nombre de Slave ne permet pas de le faire.");
        }
    }
    
    private static void attendreSlaves(int nbSlaves) throws RemoteException, MalformedURLException, InterruptedException {
        System.out.println("Master : En attente des slaves");
        /* On vérifie que tous les slaves se soient enregistrés sur le RMI, 
         * si il en manque, on attend 1 sec puis on recommence */
        boolean slavePret = false;
        while (!slavePret) {
            Thread.sleep(1000);
            slavePret = true;
            for (int i = 2; i < nbSlaves + 2; i++) {
                try {
                    Slave slave = (Slave) Naming.lookup("rmi://localhost/" + "Slave" +i);
                }
                catch (NotBoundException ex) {
                    slavePret = false;
                    break;
                }
            }
        }
    }

    public MasterMain(String dfsRootFolder, int nbSlaves) throws RemoteException {
        super();
        this.dfsRootFolder = dfsRootFolder;
        this.nbSlaves = nbSlaves;
    }
    
    private void setLeftSlave(Slave slave) {
        this.leftSlave = slave;
    }
    
    private void setRightSlave(Slave slave) {
        this.rightSlave = slave;
    }
    
    @Override
    public String getDfsRootFolder() throws RemoteException {
        return this.dfsRootFolder;
    }

    @Override
    public int getNbSlaves() throws RemoteException {
        return this.nbSlaves;
    }
    
    @Override
    public void saveFile(File file) throws RemoteException {
    	Path path = Paths.get(file.getAbsolutePath());
		try {
			byte[] contenu = Files.readAllBytes(path);
			saveBytes(file.getName(), contenu);
		} catch (IOException e) {
			e.printStackTrace();
		}

    }

    @Override
    public void saveBytes(String filename, byte[] fileContent) throws RemoteException {
        /* Création d'un nouveau thread pour que l'enregistrement ne soit pas bloquant pour le client
         * et ajout du nom du fichier dans la liste des enregistrements en cours */
        listeEnregistrementEnCours.add(filename);
        final byte[] filecontent = fileContent;
        final String fileName = filename;
        ExecutorService thread = Executors.newFixedThreadPool(1);
        thread.submit(new Runnable() {
                            public void run() { 
                                try {
                                    List<byte[]> leftList = new ArrayList<byte[]>();
                                    List<byte[]> rightList = new ArrayList<byte[]>();
                                    /* Principe:
                                    * On crée deux listes de byte[] : une pour le leftSlave et une pour le rightSlave.
                                    * On divise le tableau fileContent en 'nbSlaves' sous-tableaux.
                                    * La moitié des sous-tableaux sera affectée au leftSlave, l'autre moitié au rightSlave.
                                    */
                                    
                                    int start = 0;
                                    
                                    /* Définir la taille de base d'un sous-tableau
                                    * La méthode Math.ceil permet de récuperer la valeur entière immédiate supérieure à un nb qui n'est pas entier
                                    * Cela permet une répartition plus équitable des bytes, si le nombre total de byte n'est pas divisible par le nbSlaves
                                    */
                                    int bytesParPart = (int)Math.ceil((double)filecontent.length/nbSlaves);
                                    
                                    //Compléter la liste gauche
                                    while (start < filecontent.length/2) {
                                        leftList.add(Arrays.copyOfRange(filecontent, start, start + bytesParPart));
                                        start += bytesParPart;
                                    }
                                    
                                    //Compléter la liste droite
                                    while (start < filecontent.length-bytesParPart) {
                                        rightList.add(Arrays.copyOfRange(filecontent,start, start + bytesParPart));
                                        start += bytesParPart;
                                    }
                                    //le dernier tableau de la liste droite aura moins d'éléments que les autres
                                    //si le nombre total de byte n'est pas divisible par le nbSlaves
                                    rightList.add(Arrays.copyOfRange(filecontent,start, filecontent.length));
                                    
                                    //Affecter les deux listes aux slaves correspondants
                                    leftSlave.subSave(fileName, leftList);
                                    rightSlave.subSave(fileName, rightList);
                                    listeEnregistrementEnCours.remove(fileName);
                                    System.out.println("Sauvegarde réussie! Congrats");
                                } catch (RemoteException ex) {
                                    ex.printStackTrace();
                                }
                            }
                    });
        
    }

    @Override
    public File retrieveFile(String filename) throws RemoteException {
    	
    	File fichier = new File(dfsRootFolder,filename);
    	try {
		    FileOutputStream fos = new FileOutputStream(fichier.getAbsoluteFile()); 
			fos.write(retrieveBytes(filename));
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	    return fichier;
        
    }

    @Override
    public byte[] retrieveBytes(String filename) throws RemoteException {
        // Si ce fichier est en cours d'enregistrement, alors on attend
        while (listeEnregistrementEnCours.contains(filename)) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    	List<byte[]> listeReconst = new ArrayList<byte[]>();
    	/* Lorsque le master reçoit une demande de 'retrieveBytes' :
    	 * 		#1	Il va reconstituer la liste de tableaux de byte, à partir des deux sous-listes 
    	 * 		qu'il aura demandées à ses slaves gauche et droit. 
    	 */
        if (leftSlave != null) {
        	listeReconst.addAll(leftSlave.subRetrieve(filename));
        }
        if (rightSlave != null) {
        	listeReconst.addAll(rightSlave.subRetrieve(filename));
        }
        
        /*
         * 		#2	Il va convertir cette liste de tableaux en un seul tableau.
         */
    	int totalBytes = 0;
    	for (byte[] byteArray : listeReconst) {
    		totalBytes += byteArray.length;
    	}
    	byte[] tab = new byte[totalBytes];
    	for (int i = 0; i < listeReconst.size(); i++) {
    		System.arraycopy(listeReconst.get(i), 0, tab, i*listeReconst.get(i).length, listeReconst.get(i).length);
		}
    	System.out.println("Récupération du fichier réussie");
    	return tab;
    }

	@Override
	public long sizeOf(String filename) throws RemoteException {
		long taille = 0;
        if (leftSlave != null) {
        	taille += leftSlave.sizeOf(filename);
        }
        if (rightSlave != null) {
        	taille += rightSlave.sizeOf(filename);
        }
		return taille;
	}
}
