'''
Created on 23 juil. 2018

@author: martin.schoreisz
'''
import os, sys, csv, pyexcel_ods3, datetime
import pyexcel_ods
from Martin_Perso import Connexion_Transfert #utilisation de module perso
from Martin_Perso import Ogr_Perso #utilisation de module perso
from PyQt5.QtWidgets import QApplication #uniqumenet pour run le module
from xlrd import open_workbook
from xlutils.copy import copy
from osgeo import ogr,osr
import psycopg2 #import pour gestion erreur


#AJOUTER LA CREATION AUTO DU LINEAIRE
#IMPORTANT : A ADAPTER POUR FICHIER DE RAPPORTGE CSV ; THEORIQUEMENT OK, A VERIFIER 
#CE QUE L'ON VA FAIRE : 
#    1.AJOUTER QQCH QUI FAIT QUE L'ON NE TRAITE QUE LES FICHIERS PUBLIES OK. En commentaire pour test mais FAIT
#    2.AJOUTER QQVH qui va regarder si un fichier de rapportage est deja present dans le dossier rapporatge du sftp
#    3.penser Ã  increment le sfichier avec du del et du upadte dans le nomcomme le veut la CE.
 
class RapportageGitt(Connexion_Transfert.ConnexionBdd): #la classe hÃ©rite des attributs et methode de la classe ConnexionBdddu module perso Connexion_Transfert
    '''
   Classe pour rapporter les CBS Ã  l'Union europÃ©enne
   nÃ©cessite :
   une connexion Ã  une Bdd postgres contenant les tables rapportage.df1548710_rail_e3,rapportage.df1548710_road_e3  et cartes_bruit.n_bruit_zone_s
   une connexion au ftp du CeremaITM
   des modeles de fichiers de l'Eionet
    '''

    def __init__(self, listeDepartement,DossierCartesDeCopie=r'E:\Boulot\python3\test', parent=None):
        '''
        Constructeur
        en entree : 
        DossierCartesDeCopie : string du dossier local ou sont copier temporairement les fichiers
        listeDepartement : liste des departement du ftp a traiter
        '''
        super().__init__(typeBdd='local')#recuperation du constructeur de la classe mere
        self.listeAttributNomVoies=['numero', 'NUMERO', 'nom_voie', 'codinfra','Nom','id']
        self.listeAttributsGest=['gest', 'gestionnaire','gestion']
        self.instanceSsh=Connexion_Transfert.ConnexionSsh()#ouverture connexion SFTP
        self.DossierCartesDeCopie=DossierCartesDeCopie
        self.transfertOgr=Connexion_Transfert.Ogr2Ogr()
        self.listeDepartement=listeDepartement
        print('ouverture des connexions Ogr et Psycopg2 vers ', self.serveur, self.bdd)
        
        #liste allant etre ecrite dans le csv de suivi
        self.listeFichierSuivi=[['departement','transfert tableau rapportage', 'transfert cartes SIG', 'creation Uueid', 'creation code df710', 'transfert lineaire SIG','exporter Tableur', 'creer ligne source','exporter noise contour']]
        
        #les donnees de reprojection ; j'utilise importfromProj4 car ImportfromEpsg bug car il ne trouve pas le fichier des epsg alors que la variable gdal_data est bien declaree dans les variables sytseme
        self.epsg2154 = osr.SpatialReference()
        self.epsg2154.ImportFromProj4('+proj=lcc +lat_1=49 +lat_2=44 +lat_0=46.5 +lon_0=3 +x_0=700000 +y_0=6600000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs') #EPSG 2154
        self.epsg3857 = osr.SpatialReference()
        self.epsg3857.ImportFromProj4('+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs')# EPSG 4326
        self.epsg3035=osr.SpatialReference()
        self.epsg3035.ImportFromProj4('+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +units=m +no_defs')
        self.epsg32620=osr.SpatialReference()
        self.epsg32620.ImportFromProj4('+proj=utm +zone=20 +ellps=WGS84 +datum=WGS84 +units=m +no_defs')
        self.epsg2972=osr.SpatialReference()
        self.epsg2972.ImportFromProj4('+proj=utm +zone=22 +ellps=GRS80 +towgs84=2,2,-2,0,0,0,0 +units=m +no_defs')
        self.epsg2975=osr.SpatialReference()
        self.epsg2975.ImportFromProj4('+proj=utm +zone=40 +south +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
        self.epsg32738=osr.SpatialReference()
        self.epsg32738.ImportFromProj4('+proj=utm +zone=40 +south +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
        self.transform_2154To3035 = osr.CoordinateTransformation(self.epsg2154, self.epsg3035) #la matrice de transformation est stockÃ©e dan sune variable
        self.transform_32620To3857 = osr.CoordinateTransformation(self.epsg32620, self.epsg3857)
        self.transform_2972To3857 = osr.CoordinateTransformation(self.epsg2972, self.epsg3857)
        self.transform_2975To3857 = osr.CoordinateTransformation(self.epsg2975, self.epsg3857)
        self.transform_32738To3857 = osr.CoordinateTransformation(self.epsg32738, self.epsg3857)
        
    
    def reprojeterPoints(self,x,y,departement):
        """
        fonction pour reprojeter des points
        pourrait etre amÃ©liorer si je pouvais utiliser la fonction ImportfromEpsg de gdal, qui bug
        en entree :
        x : nombre, coordonnee x du point
        y : nombre, coordonnee y du point
        departement : string 
        en sortie :
        x1,y1 : coordonnees transformÃ©es
        """
        point = ogr.Geometry(ogr.wkbPoint) #definition de la variable selon ogr
        if x !=-2 and y != -2 :
            point.AddPoint(x, y) #ajout des coordonnÃ©es dans la variable
            if departement in ['00'+str(i) for i in list(range(1,10))]+['0'+str(i) for i in range(10,96) if  i !=20]+['02A','02B'] :
                point.Transform(self.transform_2154To3035) #application de la matrice de transformation
            elif departement == '971':
                point.Transform(self.transform_32620To3857) #application de la matrice de transformation
            elif departement == '973':
                point.Transform(self.transform_2972To3857) #application de la matrice de transformation
            elif departement == '974':
                point.Transform(self.transform_2975To3857) #application de la matrice de transformation
            elif departement == '976' :
                point.Transform(self.transform_32738To3857) #application de la matrice de transformation
            x1=round(point.GetPoint(0)[0],3)
            y1=round(point.GetPoint(0)[1],3)
        else : 
            x1,y1=-2,-2
        return x1,y1
    
    def transfertDf1548(self,departement):
        """
        transfert des fcihiers de rapportage vers la base de donnÃ©es
        en entrÃ©e : 
        departement : string, numero de departement sur 3 lettres
        en sortie :
        presenceFichier, boolean traduit si donnees d erapportage presente ou non 
        
        """
        print(departement)
        presenceFichier=None #definir le drapeau de presence d'un fichier qui va bien dans le dossier contenant les rapportages  
        for (dirname, files) in self.instanceSsh.sftp_walk('/Projet_Reussir_2017_CBS/'+departement): #parcourir un dossier
            for file in files:                   
                if file[-4:] in ('.ods','.csv') and ('tableau_rapportage' in dirname) : 
                    presenceFichier=True  
                    fichierCopie=os.path.join(self.DossierCartesDeCopie,file)
                    self.instanceSsh.sftp.get(dirname+'/'+file, fichierCopie)#tÃ©charger un fichier. attention, on doit bien mettre les stings au format du serveur ftp d'un cotÃ© et de l'ordi local de l'autre                   
                    if 'fer' in file.lower():
                        if (file.endswith('.ods')):
                            data = pyexcel_ods.get_data(fichierCopie) #recuperer les donnÃ©es  
                            liste=data.get("Reporting fer") #recuperer les valeurs de la feuille fer,ligne de titre comprise
                        else : 
                            with open(fichierCopie, newline='') as csvfile: #ouverture du fichier csv
                                dialect = csv.Sniffer().sniff(csvfile.readline())#recuperation du dialect
                                csvfile.seek(0)#je sais pas pourquoi
                                separateur=dialect.delimiter#recup du delimiter
                                lecteurCsv=csv.reader(csvfile,delimiter=separateur)#creation du reader
                                liste=list(lecteurCsv)#trasfert du reader dans la liste
                                
                        #nettoyage et mise au format de la liste
                        liste=list(filter(None,liste))#supprimer les valeurs vides de la liste
                        for elements in liste : #nettoyage pour le cas du 59 ou la premiere ligne de données non significative ne contient pas que du null mais aussi un ' '
                            if all(element in ('',' ','_') for element in elements) :
                                liste.remove(elements)
                        if liste[0][0]=='Code_Ligne_Ferroviaire' : #pour remttre a niveau les attributs issus des tableaux fissa
                            liste=[['ligne fictive1']]+liste
                            liste=[['colonneFictive']+elementsListe for elementsListe in liste ]
                        nbLignesListe=len(liste) #nombre de ligne de donnÃ©es (si prmiere kignes = titre et qu'on utulide range) Ã  verifie
                        for a in range (2,nbLignesListe): #pour traiter le cas ou le tableau est rempli avec des décimales à virgule pour le décompte de population
                            for i in [c for b in (range(2,6), range(13,65)) for c in b] :
                                if liste[a][i]=='' and i not in (2,3,4,5,13,14) : #cas des données de dénombrement
                                    liste[a][i]=0
                                elif liste[a][i]=='' and i in (2,3,4,5,13,14) : #cas des coordonnées ou trafic ou longueur
                                    liste[a][i]=-2
                                if isinstance(liste[a][i],str):
                                    try : # je met ce try except pour tout les cas comme dans le 054 route ligne 77 colonne AN où par exemple la valeur est ','. Je considère que c'est 0
                                        liste[a][i]=int(float(liste[a][i].replace(',','.')))
                                    except ValueError :
                                        print('erreur valeur liste')
                                        liste[a][i]=0
                                else :
                                    continue
                        
                        for i in range (2,nbLignesListe):#assigner chaque donnes que l'on souhaite utiliser Ã  une variable
                            #A REMETTRE SI LES TESTS FONCTIONNEpublie=liste[i][71] 
                            # A REMETTRE SI LES TESTS FONCTIONNEif publie[0].lower()=='p' : #si publie est renseigne avec un p au debut
                            voie=str(liste[i][1]) if departement not in ['014','018','027','028','037','041','045','076'] else str(liste[i][7]) #car à NC ils ont mis le nom en toute lettre en colonne B et le nom de la ligne en colonne H
                            
                            #nettoyage de la voie s'il ya des espaces à la fin : 
                            z=-1
                            if voie[z]==' ':
                                while voie[z]==' ':
                                    z=z-1
                                voie=voie[:z+1]
                            
                            natrailid1=departement+'_'+voie
                            x1=int(liste[i][2])
                            y1=int(liste[i][3])
                            x2=int(liste[i][4])
                            y2=int(liste[i][5])
                            
                            #convertir les coordonnées
                            x1,y1=self.reprojeterPoints(x1, y1,departement)
                            x2,y2=self.reprojeterPoints(x2, y2,departement)  
                            epsg='EPSG : 3035' if departement in ['00'+str(i) for i in list(range(1,11))]+['0'+str(i) for i in range(11,96) if  i !=20]+['02A','02B'] else 'EPSG : 3857'
                                                            
                            trafic=liste[i][13] if liste[i][13]!="" else -2
                            longueur=liste[i][14] if liste[i][14]!="" else -2
                            pop55LdenAgg=liste[i][21]
                            pop60LdenAgg=liste[i][22]
                            pop65LdenAgg=liste[i][23]
                            pop70LdenAgg=liste[i][24]
                            pop75LdenAgg=liste[i][25]
                            pop50LnAgg=liste[i][45]
                            pop55LnAgg=liste[i][46]
                            pop60LnAgg=liste[i][47]
                            pop65LnAgg=liste[i][48]
                            pop70LnAgg=liste[i][49]
                            pop55LdenTot=pop55LdenAgg+pop60LdenAgg+pop65LdenAgg+pop70LdenAgg+pop75LdenAgg+liste[i][15]+liste[i][16]+liste[i][17]+liste[i][18]+liste[i][19]
                            pop65LdenTot=pop65LdenAgg+pop70LdenAgg+pop75LdenAgg+liste[i][17]+liste[i][18]+liste[i][19]
                            pop75LdenTot=pop75LdenAgg+liste[i][19]
                            dwellings55=pop55LdenTot/2
                            dwellings65=pop65LdenTot/2
                            dwellings75=pop75LdenTot/2
                            
                            aire55Lden=liste[i][62] if len(liste[i])>=69 else liste[i][16]
                            aire65Lden=liste[i][63] if len(liste[i])>=69 else liste[i][17]
                            aire75Lden=liste[i][64] if len(liste[i])>=69 else liste[i][18]
                            try :
                                urlPubli=liste[i][71]
                            except IndexError :
                                urlPubli=''
                            
                            #parcourir les fichers dans la base et en deduire la liste des voie ferrees deja existante
                            self.curs.execute("SELECT DISTINCT natrailid1 FROM rapportage.df1548710_rail_e3")
                            listeClePrimaire=self.curs.fetchall()
                            
                            if  (natrailid1,) not in listeClePrimaire : #pour les voies qui ne sont pas dans la base"
                                self.curs.execute("INSERT INTO rapportage.df1548710_rail_e3 (reuc, natrailid1, antrafflow, length,lorastnox1,lorastnoy1,loraennox2,loraennoy2,loracoosys,p55ldag,p60ldag,p65ldag,p70ldag,p75ldag,p50lnag,p55lnag,p60lnag,p65lnag,p70lnag,aire55ld,aire65ld,aire75ld,p55ld,p65ld,p75ld,log55ld,log65ld,log75ld,compmetnam,comemerede) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", ('A', natrailid1,trafic,longueur,x1,y1,x2,y2,epsg,pop55LdenAgg,pop60LdenAgg,pop65LdenAgg,pop70LdenAgg,pop75LdenAgg,pop50LnAgg,pop55LnAgg,pop60LnAgg,pop65LnAgg,pop70LnAgg,aire55Lden,aire65Lden,aire75Lden,pop55LdenTot,pop65LdenTot,pop75LdenTot,dwellings55,dwellings65,dwellings75,'NMPB08',urlPubli))
                                self.connexionPsy.commit() # suavegarder les modifs
                    
                    #Cas des routes
                    else :
                        if (file.endswith('.ods')):
                            data = pyexcel_ods.get_data(fichierCopie) #transforme le fihcier en dictionnaire avec les noms de feuille en clé et les données en listes de listes
                            listeFeuille=list(data.keys()) #la liste des noms de feuiille, comme ça on cherche si Reporting routes est dedans, sinon on prend la premeiere feuille
                            if 'Reporting routes' in listeFeuille : 
                                liste=data.get("Reporting routes") #recuperer les valeurs de la feuille route,ligne de titre comprise
                            elif 'Reporting_routes' in listeFeuille :
                                liste=data.get("Reporting_routes")
                            else : 
                                liste=data.items()
                                liste=list(liste)[0][1] 
                        else : 
                            with open(fichierCopie, newline='') as csvfile: #ouverture du fichier csv
                                dialect = csv.Sniffer().sniff(csvfile.readline())#recuperation du dialect
                                csvfile.seek(0)#je sais pas pourquoi
                                separateur=dialect.delimiter#recup du delimiter
                                lecteurCsv=csv.reader(csvfile,delimiter=separateur)#creation du reader
                                liste=list(lecteurCsv)#trasfert du reader dans la liste
                        
                        #traitement de la liste pour compenser erreur logiciel ou opérateur
                        #nettoyage de la liste
                        liste=list(filter(None,liste))#supprimer les valeurs vides de la liste
                        for elements in liste : #nettoyage pour le cas du 59 ou la premiere ligne de données non significative ne contient pas que du null mais aussi un ' '
                            if all(element in ('',' ','_') for element in elements) :
                                liste.remove(elements)

                        if liste[0][0]=='Code_itineraire_europeen' : #pour remttre a niveau les attributs issus des tableaux fissa
                            liste=[['ligne_fitive1']]+[['ligne_fictive2']]+liste    
                        nbLignesListe=len(liste) #nombre de ligne de donnÃ©es (si prmiere kignes = titre et qu'on utulide range) Ã  verifie
                        for a in range (3,nbLignesListe): #pour traiter le cas ou le tableau est rempli avec des décimales à virgule
                            for i in [c for b in (range(3,7), range(12,65)) for c in b]: #pour traiter les colonnes des x y debut fin + colonne de population
                                if liste[a][i]=='' and i>13 : #cas de données vide hors coordonnées et trafic ou longueur
                                    liste[a][i]=0
                                elif liste[a][i]=='' and (i in (3,4,5,6,12,13))  : #cas de coordonnées vide ou du trafic ou de la longueur
                                    liste[a][i]=-2
                                elif isinstance(liste[a][i],str):
                                    try : # je met ce try except pour tout les cas comme dans le 054 route ligne 77 colonne AN où par exemple la valeur est ','. Je considère que c'est 0
                                        liste[a][i]=int(float(liste[a][i].replace(',','.')))
                                        print (f"str ligne {a} colonne {i}, donnes {liste[a][i]}, voie {liste[a][2]}")
                                    except ValueError :
                                        print(f"value error ligne {a} colonne {i}, donnes {liste[a][i]}, voie {liste[a][2]}")
                                        liste[a][i]=0
                                elif isinstance(liste[a][i],float) and i<62 : #pour ne pas traiter les surface qui rest des float
                                    print (f"float ligne {a} colonne {i}, donnes {liste[a][i]}, voie {liste[a][2]}")
                                    liste[a][i]=int(liste[a][i]) #ceui la c'est pour le cas du 68 avec des coordonnées type 402602.88
                                else :
                                    continue
                                    print(f"continue ligne {a} colonne {i}, donnes {liste[a][i]}, voie {liste[a][2]}")
                            
                         
                        #assigner chaque donnes que l'on souhaite utiliser Ã  une variable       
                        for i in range (3,nbLignesListe):
                        #publie=liste[i][68] A REMETTRE SI LES TESTS FONCTIONNE 
                        #if publie[0].lower()=='p' : #si publie est renseigne avec un p au debut A REMETTRE SI LES TESTS FONCTIONNE
                            voie=liste[i][2]
                            if voie!='' :
                                if voie[0] in ['A','D','N','V'] : #si les numeros de voies sont avec des '_' on le vire
                                    voie=voie.replace('_','').replace('ATMB','').replace('APRR','').replace('ASF','').replace('AREA','').replace('SANEF','').replace('SAPN','').replace('COFIROUTE','')
    
                                compteurZeros=1
                                print(voie)
                                if voie not in ('A0', 'C0', 'D0', 'N0', 'V0') :
                                    while voie[compteurZeros]=='0' :
                                        compteurZeros+=1
                                
                                voie=voie[0]+voie[compteurZeros:]
                                
                                #uniformisaton gestionnaire : pas mal d'hétérogénéïté dans les tableaux finalement
                                gest=liste[i][9] 
                                print(f'gest debut : {gest}')
                                if (gest[:2].lower() in ('cg','cd', 'rd') or gest[:9].lower()=='conseil d') and gest[:3].lower() != 'cdc' :
                                    gest='CD'
                                elif gest =='AC':
                                    gest='SCA'
                                elif gest in('APRR', 'ASF'):
                                    gest='SCA_'+gest
                                elif gest in('Etat_nonconcede ','RN' ) :
                                    gest='DIR'
                                elif gest in ('CLERMONT AUVERGNE METROPOLE', 'COMMUNAUTE AGGLO LIMOGES METROPOLE','CAT'):
                                    gest='C/I_'+gest
                                elif gest=='C/l' :
                                    gest='C/I'
                                elif gest in ('Brive-la-gaillarde','Malemort','Tulle') :
                                    gest='C_'+gest
                                elif gest in ('VC','commune') : 
                                    gest='C'       
                                
                                print(f'gest fin : {gest}')
                                
                                trafic=liste[i][12]
                                longueur=liste[i][13]
                                natroadid=departement+'_'+gest+'_'+voie
                                x1=liste[i][3]
                                y1=liste[i][4] 
                                x2=liste[i][5]
                                y2=liste[i][6]
                                
                                #convertir en epsg 3035
                                x1,y1=self.reprojeterPoints(x1, y1,departement)
                                x2,y2=self.reprojeterPoints(x2, y2,departement)
                                epsg='EPSG : 3035' if departement in ['00'+str(i) for i in list(range(1,11))]+['0'+str(i) for i in range(11,96) if  i !=20]+['02A','02B'] else 'EPSG : 3857'
                                
                                # je met le if car je constate un bug a la lecture du fichier : si tout eles colonne sont egales Ã  0 il ne met qu'un  qu'une seule colonne de valeure 0 dans la liste
                                pop55LdenAgg=liste[i][20]
                                pop60LdenAgg=liste[i][21]
                                pop65LdenAgg=liste[i][22]
                                pop70LdenAgg=liste[i][23]
                                pop75LdenAgg=liste[i][24]
                                pop50LnAgg=liste[i][44]
                                pop55LnAgg=liste[i][45]
                                pop60LnAgg=liste[i][46]
                                pop65LnAgg=liste[i][47]
                                pop70LnAgg=liste[i][48]
                                pop55LdenTot=pop55LdenAgg+pop60LdenAgg+pop65LdenAgg+pop70LdenAgg+pop75LdenAgg+liste[i][14]+liste[i][15]+liste[i][16]+liste[i][17]+liste[i][18]
                                pop65LdenTot=pop65LdenAgg+pop70LdenAgg+pop75LdenAgg+liste[i][16]+liste[i][17]+liste[i][18]
                                pop75LdenTot=pop75LdenAgg+liste[i][18]
                                dwellings55=pop55LdenTot/2
                                dwellings65=pop65LdenTot/2
                                dwellings75=pop75LdenTot/2
                                aire55Lden,aire65Lden,aire75Lden=liste[i][62], liste[i][63], liste[i][64]#pour les gars qui ont pas mis 0 et laissé vide (076)
                                try : #si l'url est pas renseigné on a un index error
                                    urlPubli=liste[i][71]
                                except IndexError :
                                    urlPubli=''
    
                                try: # le nom de rue n'est pas obligatoire, mm principe que url
                                    nomRue=liste[i][75] if liste[i][73]!='' else voie
                                except IndexError :
                                    nomRue=voie
                                                   
                                #parcourir les fichers dans la base et en deduire la liste des cle primaires deja existante
                                self.curs.execute("SELECT DISTINCT natroadid FROM rapportage.df1548710_road_e3")
                                listeClePrimaire=self.curs.fetchall()
                                    
                                if  (natroadid,) not in listeClePrimaire : 
                                    self.curs.execute("INSERT INTO rapportage.df1548710_road_e3 (gestionnaire, reuc, natroadid,natroadnam, antrafflow, length,lorostnox1,lorostnoy1,loroennox2,loroennoy2,lorocoosys,p55ldag,p60ldag,p65ldag,p70ldag,p75ldag,p50lnag,p55lnag,p60lnag,p65lnag,p70lnag,aire55ld,aire65ld,aire75ld,p55ld,p65ld,p75ld,log55ld,log65ld,log75ld,compmetnam,comemerede) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (gest,'A', natroadid,nomRue, trafic,longueur,x1,y1,x2,y2,epsg,pop55LdenAgg,pop60LdenAgg,pop65LdenAgg,pop70LdenAgg,pop75LdenAgg,pop50LnAgg,pop55LnAgg,pop60LnAgg,pop65LnAgg,pop70LnAgg,aire55Lden,aire65Lden,aire75Lden,pop55LdenTot,pop65LdenTot,pop75LdenTot,dwellings55,dwellings65,dwellings75,'NMPB08',urlPubli))
                                    self.connexionPsy.commit() # suavegarder les modifs
                    #os.remove(fichierCopie)
                    
                else :
                    presenceFichier=False if presenceFichier==False or presenceFichier==None else True #si un seul fihcier est en true il faut rester en true
        if presenceFichier : 
            self.listeFichierSuivi.append([departement,'ok'])
        else :
            self.listeFichierSuivi.append([departement,'pas_de_fichier .ods dans dossier tableau_rapportage ou sous-dossier'])
        
        return presenceFichier
    
    def transfertCartes(self,departement):
        """
        transfert des fcihiers N_BRUIT_ZBR des cartes de bruit vers la base de donnÃ©es
        en entrÃ©e : 
        departement : string, numero de departement sur 3 lettres
        """
        texteFichierSuivi='' #l'attribut qui va permettre d'ecrire le suivi. present dans toute les fonctions
        testNomDossier=['CBS','GeoStandard']
        for (dirname, files) in self.instanceSsh.sftp_walk('/Projet_Reussir_2017_CBS/'+departement): #parcourir un dossier
            for file in files:
                if file.endswith('.shp') and all(nomDossier in dirname for nomDossier in testNomDossier ) and file[:11]=='N_BRUIT_ZBR' :# si c'est une carte
                    print (dirname, file)
                    
                    #pour connaitre le gestionnaire des CBS on se base sur l'arborescence de fichier issue de Mizogeo
                    codeGestionnaire=(dirname.split(r'GeoStandard/CBS/')[1]).split('_')[0].lower()
                    if codeGestionnaire=='voies' : # si le dossier "Voie_nouvelles" a été laissé avec des voies dedans
                        if 'fer' in dirname.lower() : #si c'est une voie ferrées alors le codeGest est obligatoirement 0
                            codeGestionnaire='0'
                        else : #sinon c'est une route
                            cheminGestionnaire=(dirname.split(r'GeoStandard/CBS/Voies_nouvelles')[1])
                            codeGestionnaire=cheminGestionnaire[cheminGestionnaire.find(r'/')+1].lower()
                    dicoCodeGest={'0':'SNCF-Reseau', '1':'Etat_nonconcede','n':'Etat_nonconcede', '2':'Etat_concede','a':'Etat_concede', '3':'Conseil_Departemental','d':'Conseil_Departemental', '4':'Commune','c':'Commune','v':'Commune','5':'Metropole', '6':'Collectivite_Territoriale_Corse'}
                    gestionnaire=dicoCodeGest.get(codeGestionnaire)                
                    
                    nomFichierCourt=file[:-4]
                    fichierCopie=os.path.join(self.DossierCartesDeCopie,nomFichierCourt) #chemin comlet du fichier

                    for j in ('.shp', '.shx', '.dbf', '.prj'): #on recupere les fcihiers shape depuis le ftp
                        try : # si il manque des fichiers
                            self.instanceSsh.sftp.get(dirname+'/'+nomFichierCourt+j, fichierCopie+j)
                        except FileNotFoundError : 
                            pass
   
                    #verification si le fichier est deja dans la base
                    self.curs.execute("SELECT DISTINCT annee,'N_BRUIT_ZBR_INFRA_'||typesource||'_'||codinfra||'_'||cbstype||'_'||indicetype||'_S_'||codedept||'_'||gestionnaire FROM cartes_bruit.n_bruit_zone_s")
                    listeCartesBdd=self.curs.fetchall()
                    #print(listeCartesBdd[0])
                    if ('2017',file[:-4]+'_'+gestionnaire) not in listeCartesBdd: # si le fichier n'est pas dansla base
                        
                        try : 
                            if departement in ('971','972') : 
                                self.transfertOgr.shp2pg(self.connstringOgr, fichierCopie+'.shp',schema="cartes_bruit", SRID=32620,table="n_bruit_zone_s",geotype="MULTIPOLYGON", dims=2,creationMode="-update -append")#transfert vers la base
                            elif departement =='973' :
                                self.transfertOgr.shp2pg(self.connstringOgr, fichierCopie+'.shp',schema="cartes_bruit", SRID=2972,table="n_bruit_zone_s",geotype="MULTIPOLYGON", dims=2,creationMode="-update -append")
                            elif departement =='974' :
                                self.transfertOgr.shp2pg(self.connstringOgr, fichierCopie+'.shp',schema="cartes_bruit", SRID=2975,table="n_bruit_zone_s",geotype="MULTIPOLYGON", dims=2,creationMode="-update -append")
                            elif departement =='976' :
                                self.transfertOgr.shp2pg(self.connstringOgr, fichierCopie+'.shp',schema="cartes_bruit", SRID=4471,table="n_bruit_zone_s",geotype="MULTIPOLYGON", dims=2,creationMode="-update -append")
                            else :
                                self.transfertOgr.shp2pg(self.connstringOgr, fichierCopie+'.shp',schema="cartes_bruit",table="n_bruit_zone_s",geotype="MULTIPOLYGON", dims=2,creationMode="-update -append")
                        except :
                            texteFichierSuivi+='erreur transfert du fichier '+nomFichierCourt+' - ' #s'il y a un erreur on rempli le fihcier csv
                    """for j in ('.shp', '.shx', '.dbf', '.prj'):#suppression des fichiers copies
                        os.remove(fichierCopie+j)"""
                    
                    typesource=file.split('_')[4]
                    codinfra=file.split('_')[5] if file.split('_')[5][0] != 'C' else file.split('_')[5]+'_'+file.split('_')[6]
                    cbstype=file.split('_')[6] if file.split('_')[5][0] != 'C' else file.split('_')[7]
                    indicetype=file.split('_')[7] if file.split('_')[5][0] != 'C' else file.split('_')[8]
                    codedept=file[-7:-4]
                    print (file, typesource, codinfra, cbstype, indicetype,codedept)
                    
                    
                    self.curs.execute("UPDATE cartes_bruit.n_bruit_zone_s SET gestionnaire=%s WHERE typesource=%s AND codinfra=%s AND cbstype=%s AND indicetype=%s AND codedept=%s AND gestionnaire IS NULL",(gestionnaire,typesource,codinfra,cbstype,indicetype,codedept))
                    self.connexionPsy.commit()
                    
        if texteFichierSuivi== '' :
            self.listeFichierSuivi[len(self.listeFichierSuivi)-1].append('ok')
        else :
            self.listeFichierSuivi[len(self.listeFichierSuivi)-1].append(texteFichierSuivi)
    
    def creationUueid(self,departement):
        """
        Creation de l'id unique europeen par recuperation ou creation
        en entrÃ©e : 
        departement : string, numero de departement sur 3 lettres
        """
        print ('creation Uueid depuis n_bruit_zone_s...')
        #POUR LES VOIES ISSUES DES CBS 2012
        self.curs.execute("SELECT DISTINCT codinfra, uueid FROM cartes_bruit.n_bruit_zone_s WHERE codedept=%s and annee='2017'  AND codinfra IS NOT NULL",(departement,)) #recuperation des iduniques existnat ATTENTION : SUPPRIMER LA CONDITION IS NOT NULL : C'EST POUR NE PAS BLOQUER SUR UNE ERREUR DANS LE 042 SUR A89 C LN et C LN
        listeUueid=self.curs.fetchall()#transfert dans une liste ; on doit poouvoir s'en passer, les cureseur sont iterables

        for i in range(len(listeUueid)): #insertion dans la table
            if listeUueid[i][0].isdigit():#si c'esune voie ferree
                self.curs.execute("UPDATE rapportage.df1548710_rail_e3 SET uniqrailid=%s WHERE left(natrailid1,3)=%s AND substr(natrailid1,5)=%s",(listeUueid[i][1], departement, listeUueid[i][0]))#on met Ã  jour latable des voies ferrees
            else:#sinon c'est une route
                self.curs.execute("UPDATE rapportage.df1548710_road_e3 SET uniqroadid=%s WHERE left(natroadid,3)=%s AND lower(natroadnam)=lower(%s)",(listeUueid[i][1], departement, listeUueid[i][0]))
        self.connexionPsy.commit()#enregistrement des donnÃ©es
        print('fait')
        
        print('creation Uueid manquant...')
        # POUR LES VOIES NOUVELLES
        for (dirname, files) in self.instanceSsh.sftp_walk('/Projet_Reussir_2017_CBS/'+departement): #parcourir un dossier
            for file in files:
                    if file.endswith('.csv') and ('tables_voies_nouvelles' in dirname):
                        fichierCopie=os.path.join(self.DossierCartesDeCopie,file)
                        self.instanceSsh.sftp.get(dirname+'/'+file,fichierCopie)# tÃ©charger le fichier des voies nouvelles depuis le ftp
                    
                    #creation des uueid issus des voies nouvelles route ; pour le fer c'est plus simple car un seul gestionnaire, la fonction postgres fait tout toute seule
                        with open(fichierCopie, newline='') as csvfile: #ouverture du fichier csv
                            dialect = csv.Sniffer().sniff(csvfile.readline())#recuperation du dialect
                            csvfile.seek(0)#je sais pas pourquoi
                            separateur=dialect.delimiter#recup du delimiter
                            lecteurCsv=csv.reader(csvfile,delimiter=separateur)#creation du reader
                            donneesCsv=list(lecteurCsv)#trasfert du reader dans une liste
        
                        for i in range (1,len(donneesCsv)): #parcour de la liste pour creation du gestionnaire dans Bdd
                            voie=donneesCsv[i][1]
                            gest=donneesCsv[i][2]
                            print(voie, gest)
                            #ici on pourrait ajouter un test sur le departement, genre if donneesCsv[i][3]==int(departement)
                            self.curs.execute("UPDATE rapportage.df1548710_road_e3 SET gestionnaire=%s WHERE uniqroadid IS NULL AND left(natroadid,3)=%s AND lower(natroadnam)=lower(%s) AND gestionnaire IS NULL",(gest,departement,voie))
                            self.connexionPsy.commit()
                        
                        os.remove(fichierCopie)
        texteFichierSuivi=''
        for typeSource in ['route', 'fer']:   
            try : 
                self.curs.execute("SELECT rapportage.creer_uueid(%s,%s)",(departement,typeSource))#creation des uueid par appel de la fonction postgres perso
                self.connexionPsy.commit()
                texteFichierSuivi+=typeSource+' ok '
            except psycopg2.InternalError:
                print('erreur psycopg2')
                texteFichierSuivi+=typeSource+'psycopg2 internal erreur'
        self.listeFichierSuivi[len(self.listeFichierSuivi)-1].append(texteFichierSuivi) 
        print('fait')
            
    def creerCodeDf710(self, departement):
        """
        Creation du nom de ppbe dans le Coverage
        en entrÃ©e : 
        departement : string, numero de departement sur 3 lettres
        """
        print('creation codedf710...')
        try : 
            self.curs.execute("SELECT rapportage.creer_codedf710(%s)",(departement,))#appel de la fonction postgis qui va bien
            self.connexionPsy.commit()
            self.listeFichierSuivi[len(self.listeFichierSuivi)-1].append('ok')
        except psycopg2.InternalError : 
            self.listeFichierSuivi[len(self.listeFichierSuivi)-1].append('psycopg2 internal erreur ; codedf710')
        print('fait')

    def affectationLineaire(self,departement):
        """
        Affectation de la geometrie du lineaire au df1548
        en entrÃ©e : 
        departement : string, numero de departement sur 3 lettres
        """
        print('affectation lineaire...')
        texteFichierSuivi=''
        
        #test pour ne pas traites les voies dejÃ  renseignees
        self.curs.execute("SELECT natroadnam, gestionnaire FROM rapportage.df1548710_road_e3 WHERE left(natroadid,3)=%s AND geom IS NOT NULL",(departement,))
        listeVoiesAvecGeometrie=self.curs.fetchall()
        listeVoiesAvecGeometrie=[(element[0].replace(' (Métropole)','').replace(' (MÃ©tropole)',''),element[1]) if element[0][0]=='D' and element[1]=='C/I' else (element[0],element[1]) for element in listeVoiesAvecGeometrie ]
        #print(listeVoiesAvecGeometrie) #pour verif
        compteurGeomInvalide=0 # pour compter le nb de geométrie invalide dans le fichier de lineaire
        
        for (dirname, files) in self.instanceSsh.sftp_walk('/Projet_Reussir_2017_CBS/'+departement): #parcourir un dossier
            for file in files:
                if file.endswith('.shp') and ('lineaire' in dirname): #si c'est un fichier lineaire
                    nomFichierCourt=file[:-4]
                    fichierCopie=os.path.join(self.DossierCartesDeCopie,nomFichierCourt)
                    for j in ('.shp', '.shx', '.dbf', '.prj'): #on recupere les fcihiers shape depuis le ftp
                        self.instanceSsh.sftp.get(dirname+'/'+nomFichierCourt+j, fichierCopie+j)
                    
                    fichier=Ogr_Perso.DonneesShapefile(fichierCopie+'.shp') #on ouvre une instance pour coionnaitre les propriete du fichier
                    listeAttributsFichier=fichier.listeAttributs()#on recupere la  liste des attributs du fichier shape
                    listeAttributsFichier=[attribut.lower() for attribut in listeAttributsFichier] #on passe en minuscule pour virer les pb de casses
                    for nomAttributs in self.listeAttributNomVoies : #on parcour la lite des nom d'attriibut prÃ©-dintifiÃ©s
                        if nomAttributs.lower() in listeAttributsFichier : #si le nom est dans la liste
                            for feature in fichier.layer :
                                geometrie=feature.GetGeometryRef()
                                try :
                                    geometrie=geometrie.ExportToWkt() #le seul moyen que j'ai trouvÃ© pour passer la geom dans postgres pour un seul objet c'est de passer la WKT depuis python puis recovertir dans pg
                                except AttributeError : # si la géométrie est de type NoneType
                                    compteurGeomInvalide+=1 #on compte le nb de geometrie pourrie
                                    continue # on passe à l'objet suivant
                                voie=str(feature.GetField(nomAttributs))
                                
                                #recuperer le gestionnaire
                                for nomGest in self.listeAttributsGest :
                                    try : 
                                        #print (file,voie, nomGest)
                                        gest=feature.GetField(nomGest)
                                        gest= 'CD' if gest.isdigit() else gest
                                        break #pour sortir de la boucle si c'est le bon d'attribut
                                    except : #si le nom d'attribut n'est pas le bon
                                        gest=None 
                                        #print (file,voie,'pas attribut gestionnaire egal a ',nomGest)  
                                
                                #nettoyage de la variable voie comme dans la focntions transfertDf1548, avec nettoyage d'eventuel nom de SCA en plus
                                if voie[0] in ['A','D','N','V'] : #si les numeros de voies sont avec des '_' on le vire
                                    voie=voie.replace('_','').replace('ATMB','').replace('APRR','').replace('ASF','').replace('AREA','').replace('SANEF','').replace('SAPN','').replace('COFIROUTE','')
                                    compteurZeros=1
                                    while voie[compteurZeros]=='0' :
                                        compteurZeros+=1
                                        voie=voie[0]+voie[compteurZeros:]                         
                                                           
                                if (voie,gest) not in listeVoiesAvecGeometrie: #si la voie n'est pas deja traitee : ATTENTION AVEC LES VOIES nommÃ©es DX (MÃ©tropole) : on repasse Ã  cahque fois
                                    try : 
                                        if voie[0].isalpha(): #si la 1er caracteres est alphanumerique (alphabet) c'est que c'est une route
                                            if gest=='' or gest==None :
                                                self.curs.execute("UPDATE rapportage.df1548710_road_e3 SET geom=CASE WHEN geom IS NULL THEN ST_Multi(ST_Force2D(ST_Transform(ST_GeomFromText(%s,2154),3035))) WHEN geom IS NOT NULL THEN ST_Multi(St_Union(geom,ST_Force2D(ST_Transform(ST_GeomFromText(%s,2154),3035)))) END::geometry(MULTILINESTRING,3035) WHERE lower(natroadnam)=lower(%s) AND left(natroadid,3)=%s",(geometrie,geometrie,voie,departement))
                                            else :
                                                #print (voie, gest)
                                                self.curs.execute("UPDATE rapportage.df1548710_road_e3 SET geom=CASE WHEN geom IS NULL THEN ST_Multi(ST_Force2D(ST_Transform(ST_GeomFromText(%s,2154),3035))) WHEN geom IS NOT NULL THEN ST_Multi(St_Union(geom,ST_Force2D(ST_Transform(ST_GeomFromText(%s,2154),3035)))) END::geometry(MULTILINESTRING,3035) WHERE CASE WHEN natroadnam LIKE '%%D%%(MÃ©tropole)%%' AND gestionnaire='C/I' THEN lower(replace(split_part(natroadnam,' (MÃ©tropole)',1),'_','')) WHEN natroadnam LIKE '%%D%%(Métropole)%%' AND gestionnaire='C/I' THEN lower(replace(split_part(natroadnam,' (Métropole)',1),'_','')) ELSE lower(natroadnam) END::character varying=lower(%s) AND left(natroadid,3)=%s AND CASE WHEN lower(left(gestionnaire,3)) IN ('dir', 'eta') THEN 'dir' WHEN lower(left(gestionnaire,1)) ='c' AND lower(left(gestionnaire,2))!='cd' THEN 'commune/interco' ELSE gestionnaire END::character varying  = CASE WHEN lower(left(%s,1))='n' AND lower(left(%s,2)) !='cr'  THEN 'dir' WHEN lower(left(%s,1))='d' and (lower(left(%s,2)) ='c/' OR lower(left(%s,1)) = 'i') THEN 'commune/interco' WHEN lower(left(%s,1))='c' OR lower(left(%s,2))='vc' then 'commune/interco' WHEN lower(left(%s,1))='a' and lower(left(%s,1))='d' THEN 'dir' ELSE %s END::character varying",(geometrie,geometrie,voie,departement,voie,gest,voie,gest,gest,voie,gest,voie,gest,gest)) #un case when dans le where pour gere si les noms de gestionnaire ne sont pas tout a fait les memes                                                
                                        else : #sinon c'est du fer
                                            self.curs.execute("UPDATE rapportage.df1548710_rail_e3 SET geom=CASE WHEN geom IS NULL THEN ST_Multi(ST_Force2D(ST_Transform(ST_GeomFromText(%s,2154),3035))) WHEN geom IS NOT NULL THEN ST_Multi(St_Union(geom,ST_Force2D(ST_Transform(ST_GeomFromText(%s,2154),3035)))) END::geometry(MULTILINESTRING,3035) WHERE substr(natrailid1,5)=%s AND left(natrailid1,3)=%s",(geometrie,geometrie,voie,departement))
                                        self.connexionPsy.commit()
                                        texteFichierSuivi+=voie+ ' : ok \n' if voie not in texteFichierSuivi else ''
                                    except Exception as e : #si erreur on renseign le fcihier de suivi
                                        print("type error: " + str(e)) 
                                        texteFichierSuivi+=voie + ' erreur de mise Ã  jour de la geometrie '
                            break #des qu el'on a trouve un attribut qui va bien on sort du parcours des attributs
                        # a la fin on regarde si des geometries etaient invalides
                        if compteurGeomInvalide!=0 : 
                            texteFichierSuivi+= 'nb geom invalides = ' + str(compteurGeomInvalide)
                            
                    else : #si le nom n'est pas dabs la liste
                        texteFichierSuivi+= 'pas de nom d\'attribut prevu dans le fichier ' + nomFichierCourt
                    del fichier
                    """for j in ('.shp', '.shx', '.dbf', '.prj'):#suppression des fichiers copies
                        os.remove(fichierCopie+j)"""
        self.listeFichierSuivi[len(self.listeFichierSuivi)-1].append(texteFichierSuivi)
        
        print('fait')
    
    def creationLineaire(self):
        requete="WITH deptl AS (  SELECT l.pk, l.gest, l.dept, l.codinfra, l.gestionnai, l.nature, l.geom,r.gestionnaire, r.natroadnam, r.natroadid,r.uniqroadid    FROM rapportage.lineaire_med AS l, rapportage.df1548710_road_e3 AS r    WHERE r.geom IS NULL AND      l.dept = left(r.natroadid,3) AND      lower(l.gest) IN ('cd','cd_') AND lower(r.gestionnaire)='cd'      AND CASE WHEN right(l.codinfra,1) !~* '[A-Z]' AND l.codinfra !~* 'D0[0-9]{1,}[A-Z]{1,}[0-9]{1,}' AND l.codinfra !~* 'D[1-9]{1,}[A-Z]{1,}[1-9]{1,}' THEN lower(left(replace(l.codinfra,' ',''),1)||to_char(to_number(substring(l.codinfra,2),'FM9999'),'FM9999') )               WHEN l.codinfra ~* 'D0[0-9]{1,}[A-Z]{1,}[0-9]{1,}' THEN lower(left(l.codinfra,1)||to_char(to_number(substring(l.codinfra from'0{1,}[0-9]{1,}'),'FM9999'),'FM9999')||substring(substring(l.codinfra from'[1-9]{1}[A-Z]{1,}[0-9]{1,}'),2))               WHEN l.codinfra ~* 'D[1-9]{1,}[A-Z]{1,}[1-9]{1,}' THEN lower(l.codinfra)               ELSE lower(left(l.codinfra,1)||to_char(to_number(substring(l.codinfra,2,length(l.codinfra)-2),'FM9999'),'FM9999')||right(l.codinfra,1)) END = lower(replace(r.natroadnam,' ',''))), natnl AS ( SELECT l.pk, l.gest, l.dept, l.codinfra, l.gestionnai, l.nature, l.geom,r.gestionnaire, r.natroadnam, r.natroadid,r.uniqroadid    FROM rapportage.lineaire_med AS l, rapportage.df1548710_road_e3 AS r    WHERE r.geom IS NULL AND      l.dept = left(r.natroadid,3) AND      ((left(lower(r.gestionnaire),3)='dir' AND left(lower(r.natroadnam),1) ='a') OR left(lower(r.natroadnam),1) ='n') AND ((l.gest = 'etat' AND left(l.gestionnai,1) ='D') OR lower(left(l.codinfra,1))='n') AND      left(l.codinfra,1)||to_char(to_number(substring(l.codinfra,2),'FM9999'),'FM9999') = r.natroadnam), sca AS (  SELECT l.pk, l.gest, l.dept, l.codinfra, l.gestionnai, l.nature, l.geom,r.gestionnaire, r.natroadnam, r.natroadid,r.uniqroadid    FROM rapportage.lineaire_med AS l, rapportage.df1548710_road_e3 AS r    WHERE r.geom IS NULL AND      l.dept = left(r.natroadid,3) AND      left(l.codinfra,1)||to_char(to_number(substring(l.codinfra,2),'FM9999'),'FM9999') = r.natroadnam AND      left(lower(r.gestionnaire),3)='sca' AND left(lower(r.natroadnam),1)='a' AND l.gest = 'etat' AND left(l.gestionnai,1) != 'D' AND nature = 'Autoroute'), comm AS (  SELECT l.pk, l.gest, l.dept, l.codinfra, l.gestionnai, l.nature, l.geom,r.gestionnaire, r.natroadnam, r.natroadid,r.uniqroadid    FROM rapportage.lineaire_med AS l, rapportage.df1548710_road_e3 AS r    WHERE r.geom IS NULL AND      l.dept = left(r.natroadid,3) AND      lower(l.codinfra) = lower(r.natroadnam) AND      lower(left(l.codinfra,1)) = 'c' and lower(left(r.natroadnam,1))= 'c'), collect_corse AS (  SELECT l.pk, l.gest, l.dept, l.codinfra, l.gestionnai, l.nature, l.geom,r.gestionnaire, r.natroadnam, r.natroadid,r.uniqroadid    FROM rapportage.lineaire_med AS l, rapportage.df1548710_road_e3 AS r    WHERE r.geom IS NULL AND      l.dept = left(r.natroadid,3) AND      l.codinfra=r.natroadnam AND      r.gestionnaire='CTC' AND l.gest='collectivite'), autre AS ( SELECT l.pk, l.gest, l.dept, l.codinfra, l.gestionnai, l.nature, l.geom,r.gestionnaire, r.natroadnam, r.natroadid,r.uniqroadid    FROM rapportage.lineaire_med AS l, rapportage.df1548710_road_e3 AS r    WHERE r.geom IS NULL AND      l.dept = left(r.natroadid,3) AND      left(l.codinfra,1)||to_char(to_number(substring(l.codinfra,2),'FM9999'),'FM9999') = r.natroadnam AND      lower(left(l.codinfra,1)) in ('m','t') and lower(left(r.natroadnam,1))in ('m','t')), total AS (  SELECT * FROM deptl UNION SELECT * FROM natnl UNION SELECT * FROM sca UNION SELECT * FROM comm UNION SELECT * FROM autre UNION SELECT * FROM collect_corse), geom_groupe AS (  SELECT ST_Union(geom) As geom, uniqroadid    FROM total    GROUP BY uniqroadid) UPDATE rapportage.df1548710_road_e3 AS r  SET geom=st_transform(g.geom,3035)  FROM geom_groupe AS g  WHERE r.uniqroadid = g.uniqroadid ;  WITH fer AS (  SELECT l.pk, l.gest, l.dept, l.codinfra, l.gestionnai, l.nature, l.geom, r.natrailid1,r.uniqrailid    FROM rapportage.lineaire_med AS l, rapportage.df1548710_rail_e3 AS r    WHERE r.geom IS NULL AND      l.dept = left(r.natrailid1,3) AND      lower(l.gest) ='sncf-reseau' AND      l.codinfra = split_part(r.natrailid1,'_',2)), geom_groupe AS (  SELECT ST_Union(geom) As geom, uniqrailid    FROM fer    GROUP BY uniqrailid) UPDATE rapportage.df1548710_rail_e3 AS r  SET geom=st_transform(g.geom,3035)  FROM geom_groupe AS g  WHERE r.uniqrailid = g.uniqrailid ;  "
        self.curs.execute(requete)
        self.connexionPsy.commit()
        
        
                           
    def exporterTableurs(self, dossierModele,typeDF,typeCreation='del'):
        """
        fonction pour exporter depuis une BDD vers un fichier excel formater selon exigence CE
        en entree:
        dossierModele : string, le dossier contenant les fichier source vide fournit par CE (chemin complet)
        fichierSortie : string, le dossier du fichier de sortie (chemin complet)
        typeDF : liste de string(5), liste de chaine de caractere precisant le type de DF : DF1_5, DF4_8, DF7_10
        typeCreation : string, traduit si le fcihier de rapportage en sortie est un premier fichier ou une mise Ã  jour: 'del' par defaut ou 'upd'+date pour une MaJ
        """
        prefixeNomFichierSortie='FR_A_' #le A peut changer, voir avec DGPR ou Cerema ITM
        aujourdhui=datetime.datetime.now() #pour le nom des fichier update 
        mois='0'+str(aujourdhui.month) if aujourdhui.month<10 else str(aujourdhui.month)
        jour='0'+str(aujourdhui.day) if aujourdhui.month<9 else str(aujourdhui.month)
        suffixeNomFichierSortie=typeCreation if typeCreation=='del' else 'upd'+str(aujourdhui.year)[2:]+mois+jour #en foncion de si c'est la premiere livraison ou non Ã  la CE
        listeFichierSortie=[]
        texteFichierSuivi=''
        
        for DfType in typeDF:
            fichierModele=os.path.join(dossierModele,'NoiseDirective'+DfType+'.xls') if DfType in ['DF1_5', 'DF4_8'] else os.path.join(dossierModele,'NoiseDirective'+DfType[:2]+'_'+DfType[2:]+'_APCoverage.xls')  #recuperation des fihciers modeles issu de la CE. actuellement il reprennent le nom des DFs
            nomFichierSortie=prefixeNomFichierSortie+DfType+'_2017_'+suffixeNomFichierSortie+'.xls'
            try : 
                fichierAOuvrir=open_workbook(fichierModele) #issu du module xlrd de lecture des fchiers excels
                texteFichierSuivi+=DfType +' : ouverture fichier ok - '
            except :
                texteFichierSuivi+=DfType + ' : le fichier '+fichierModele,'du dossier ',dossierModele,'  ne peut pas etre ouvert - '
                print(f"{DfType} + ' : le fichier '+{fichierModele},'du dossier ',{dossierModele},'  ne peut pas etre ouvert - ")
                continue
            fichierACree=copy(fichierAOuvrir) #la c'est xlutils qui est utilisÃ© : on bosse sur une copie memoire du fichier en fait
            fichierSortie=os.path.join(self.DossierCartesDeCopie, nomFichierSortie)
            ligneEnCoursRoute=1
            ligneEnCoursFer=1
            #creation des variables reutilise en fonction du DF en cours
            if DfType=='DF1_5' :
                requeteRoute="SELECT reuc,eurroadid,natroadid,natroadnam,uniqroadid,antrafflow,length,lorostnox1,lorostnoy1,loroennox2,loroennoy2,lorocoosys FROM rapportage.df1548710_road_e3 WHERE geom IS NOT NULL ORDER BY uniqroadid"
                requeteFer="SELECT reuc,natrailid1,natrailid2,uniqrailid,antrafflow,length,lorastnox1,lorastnoy1,loraennox2,loraennoy2,loracoosys FROM rapportage.df1548710_rail_e3 WHERE geom IS NOT NULL ORDER BY uniqrailid"
                numeroFeuilleRoute=2
                numeroFeuilleFer=3
                nbColonneRoute=12
                nbColonneFer=11
            elif DfType=='DF4_8' :
                requeteRoute="SELECT reuc,uniqroadid,p50ldag,p55ldag,p60ldag,p65ldag,p70ldag,p75ldag,p50ldagsi,p55ldagsi,p60ldagsi,p65ldagsi,p70ldagsi,p75ldagsi,p50ldagqf,p55ldagqf,p60ldagqf,p65ldagqf,p70ldagqf,p75ldagqf,p45lnag,p50lnag,p55lnag,p60lnag,p65lnag,p70lnag,p45lnagsi,p50lnagsi,p55lnagsi,p60lnagsi,p65lnagsi,p70lnagsi,p45lnagqf,p50lnagqf,p55lnagqf,p60lnagqf,p65lnagqf,p70lnagqf,aire55ld,aire65ld,aire75ld,p55ld,p65ld,p75ld,log55ld,log65ld,log75ld,refmaps,comemerede,compmetnam FROM rapportage.df1548710_road_e3 WHERE geom IS NOT NULL ORDER BY uniqroadid"
                requeteFer="SELECT reuc,uniqrailid,p50ldag, p55ldag, p60ldag, p65ldag, p70ldag, p75ldag, p50ldagsi,p55ldagsi, p60ldagsi, p65ldagsi, p70ldagsi, p75ldagsi, p50ldagqf,p55ldagqf, p60ldagqf, p65ldagqf, p70ldagqf, p75ldagqf, p45lnag,p50lnag, p55lnag, p60lnag, p65lnag, p70lnag, p45lnagsi, p50lnagsi,p55lnagsi, p60lnagsi, p65lnagsi, p70lnagsi, p45lnagqf, p50lnagqf,p55lnagqf, p60lnagqf, p65lnagqf, p70lnagqf,aire55ld,aire65ld,aire75ld, p55ld, p65ld, p75ld, log55ld, log65ld, log75ld, refmaps,comemerede, compmetnam FROM rapportage.df1548710_rail_e3 WHERE geom IS NOT NULL ORDER BY uniqrailid"
                numeroFeuilleRoute=6
                numeroFeuilleFer=7
                nbColonneRoute=50
                nbColonneFer=50
            elif DfType=='DF7_10' : # A VERIFIER
                requeteRoute="SELECT reuc,uniqroadid,codedf710 FROM rapportage.df1548710_road_e3  WHERE geom IS NOT NULL ORDER BY uniqroadid"
                requeteFer="SELECT reuc,uniqrailid,codedf710 FROM rapportage.df1548710_rail_e3  WHERE geom IS NOT NULL ORDER BY uniqrailid"
                numeroFeuilleRoute=2
                numeroFeuilleFer=3
                nbColonneRoute=3
                nbColonneFer=3
            try : #creation des fichiers excel route
                self.curs.execute(requeteRoute) #recuperation des donnees de la Bdd
                feuilleRoute=fichierACree.get_sheet(numeroFeuilleRoute)#initialisation d ela feuille excel (xlutils)
                for rec in self.curs : #parcours du curseur
                    for colonne in range(nbColonneRoute) : #parcours des colonnes
                        feuilleRoute.write(ligneEnCoursRoute,colonne,rec[colonne]) #ecriture des donnes dans chaque colonne
                    ligneEnCoursRoute+=1
                texteFichierSuivi+='ecriture tableur route ok '
            except :
                texteFichierSuivi+='erreur ecriture tableur route'
                print('erreur ecriture tableur route {DfType}')
            try :#pareil popur le fer
                self.curs.execute(requeteFer)
                feuilleFer=fichierACree.get_sheet(numeroFeuilleFer)
                for rec in self.curs :
                    for colonne in range(nbColonneFer) :  
                        feuilleFer.write(ligneEnCoursFer,colonne,rec[colonne])
                    ligneEnCoursFer+=1
                fichierACree.save(fichierSortie)
                texteFichierSuivi+='ecriture tableur fer ok \n'   
            except :
                texteFichierSuivi+='erreur ecriture tableur fer \n'
                print('erreur ecriture tableur fer {DfType}')
            listeFichierSortie.append(nomFichierSortie)#permettra ensuite de transfere les fichiers sur le ftp
        
        self.listeFichierSuivi[len(self.listeFichierSuivi)-1].append(texteFichierSuivi)
        return listeFichierSortie
            
    def creerLignesSources(self):
        """
        exporter les lignes sources de la base popstgis au format demandÃ© par l'UE (fer et route)
        """
        try : #on crÃ©e par extraction de la Bdd : route
            self.transfertOgr.pg2shp(self.connstringOgr, os.path.join(self.DossierCartesDeCopie,'FR_A_MRoad_Source.shp'),"SELECT uniqroadid AS \"UnRoad_ID\", 'A'::character varying AS \"RepEntUnCD\", eurroadid AS \"EURoadId\", natroadid AS \"NRoadID\", natroadnam AS \"NRoadName\", antrafflow AS \"AnualTFlow\", length AS \"Length\", geom FROM rapportage.df1548710_road_e3 WHERE geom IS NOT NULL AND substr(uniqroadid,8,2) NOT IN ('97','99')")
            self.transfertOgr.pg2shp(self.connstringOgr, os.path.join(self.DossierCartesDeCopie,'FR_A_MRoad_Source_Overseas.shp'),"SELECT uniqroadid AS \"UnRoad_ID\", 'A'::character varying AS \"RepEntUnCD\", eurroadid AS \"EURoadId\", natroadid AS \"NRoadID\", natroadnam AS \"NRoadName\", antrafflow AS \"AnualTFlow\", length AS \"Length\", geom FROM rapportage.df1548710_road_e3 WHERE geom IS NOT NULL AND substr(uniqroadid,8,2) IN ('97','99')")
            texteFichierSuivi='routes ok - '
        except :
            texteFichierSuivi='erreur routes - '
        try : #fer
            self.transfertOgr.pg2shp(self.connstringOgr, os.path.join(self.DossierCartesDeCopie,'FR_A_MRail_Source.shp'),"SELECT uniqrailid AS \"UnRail_ID\", 'A'::character varying AS \"RepEntUnCD\", natrailid1 AS \"NRailID1\", natrailid2 AS \"NRailID2\", NULL AS \"NRailName\", antrafflow AS \"AnualTFlow\", length AS \"Length\",geom FROM rapportage.df1548710_rail_e3")
            texteFichierSuivi+='fer ok'
        except :
            texteFichierSuivi+='erreur fer'
        self.listeFichierSuivi[len(self.listeFichierSuivi)-1].append(texteFichierSuivi)   
        
    def creerNoiseContours(self): #VERIFIER SI IL FAUT BIEN UN SEUL OBJET PAR ISOPHONE POUR FRANCE ENTIERE
        """
        exporter les donnees de cbs de la base postgis au format demandÃ© par l'UE (fer et route, lden et ln)
        """
        try :  #on crÃ©e par extraction de la Bdd : route Lden et Ln
            #self.transfertOgr.pg2shp(self.connstringOgr, os.path.join(self.DossierCartesDeCopie,'FR_A_MRoad_Lden.shp'), "SELECT geom,'FR'::character varying AS \"CTRYID\", 'A'::character varying AS \"RepEntUnCD\", to_number(legende,'99')::integer AS \"DB_Low\",(to_number(legende,'99')+4)::integer  AS \"DB_High\", ST_area(geom)::integer AS \"SHAPE_Area\" FROM cartes_bruit.n_bruit_zone_s WHERE annee='2017' AND typesource='R' AND cbstype='A' AND indicetype='LD' AND codedept NOT IN ('971','973','974','976')",3035)
            #self.transfertOgr.pg2shp(self.connstringOgr, os.path.join(self.DossierCartesDeCopie,'FR_A_MRoad_Ln.shp'), "SELECT geom,'FR'::character varying AS \"CTRYID\", 'A'::character varying AS \"RepEntUnCD\", to_number(legende,'99')::integer  AS \"DB_Low\",(to_number(legende,'99')+4)::integer  AS \"DB_High\",ST_area(geom)::integer AS \"SHAPE_Area\" FROM cartes_bruit.n_bruit_zone_s WHERE annee='2017' AND typesource='R' AND cbstype='A' AND indicetype='LN' AND codedept NOT IN ('971','973','974','976')",3035)
            self.transfertOgr.pg2shp(self.connstringOgr, os.path.join(self.DossierCartesDeCopie,'FR_A_MRoad_Lden_Overseas.shp'), "SELECT geom,'FR'::character varying AS \"CTRYID\", 'A'::character varying AS \"RepEntUnCD\", to_number(legende,'99')::integer AS \"DB_Low\",(to_number(legende,'99')+4)::integer  AS \"DB_High\", ST_area(geom)::integer AS \"SHAPE_Area\" FROM cartes_bruit.n_bruit_zone_s WHERE annee='2017' AND typesource='R' AND cbstype='A' AND indicetype='LD' AND codedept IN ('971','973','974','976')",3857)
            self.transfertOgr.pg2shp(self.connstringOgr, os.path.join(self.DossierCartesDeCopie,'FR_A_MRoad_Ln_Overseas.shp'), "SELECT geom,'FR'::character varying AS \"CTRYID\", 'A'::character varying AS \"RepEntUnCD\", to_number(legende,'99')::integer  AS \"DB_Low\",(to_number(legende,'99')+4)::integer  AS \"DB_High\", ST_area(geom)::integer AS \"SHAPE_Area\" FROM cartes_bruit.n_bruit_zone_s WHERE annee='2017' AND typesource='R' AND cbstype='A' AND indicetype='LN' AND codedept IN ('971','973','974','976')",3857)
            texteFichierSuivi='routes ok - '
        except : 
            texteFichierSuivi='erreur routes - '
        try : #fer
            #self.transfertOgr.pg2shp(self.connstringOgr, os.path.join(self.DossierCartesDeCopie,'FR_A_MRail_Lden.shp'), "SELECT geom,'FR'::character varying AS \"CTRYID\", 'A'::character varying AS \"RepEntUnCD\", to_number(legende,'99')::integer  AS \"DB_Low\",(to_number(legende,'99')+4)::integer  AS \"DB_High\", ST_area(geom)::integer AS \"SHAPE_Area\" FROM cartes_bruit.n_bruit_zone_s WHERE annee='2017' AND typesource='F' AND cbstype='A' AND indicetype='LD'",3035)
            #self.transfertOgr.pg2shp(self.connstringOgr, os.path.join(self.DossierCartesDeCopie,'FR_A_MRail_Ln.shp'), "SELECT geom,'FR'::character varying AS \"CTRYID\", 'A'::character varying AS \"RepEntUnCD\", to_number(legende,'99')::integer  AS \"DB_Low\",(to_number(legende,'99')+4)::integer  AS \"DB_High\", ST_area(geom)::integer AS \"SHAPE_Area\" FROM cartes_bruit.n_bruit_zone_s WHERE annee='2017' AND typesource='F' AND cbstype='A' AND indicetype='LN'",3035)
            texteFichierSuivi='fer ok'
        except : 
            texteFichierSuivi='erreur fer'
        self.listeFichierSuivi[len(self.listeFichierSuivi)-1].append(texteFichierSuivi)
    
    def creerFichierSuivi(self):
        """
        fonction pour crÃ©er le fichier csv de suivi des erreurs du tranfert et de la crÃ©ation
        en entree:
        pas d'entree explicite mais utilisation de l'attribut self.listeFichierSuivi
        """
        with open(os.path.join(self.DossierCartesDeCopie,'tableauSuiviCreationRapportage.csv'),'w',newline='') as csvfile:#ecriture classique de manipulation des csv
            csvwriter = csv.writer(csvfile, delimiter=';')
            csvwriter.writerows(self.listeFichierSuivi)
    
    def uploadRapportageSurFtp(self, listeFichierDf):
        """
        fonction de transfert sur le ftp des fichiers crees
        en entree : 
        listeFichierDf : liste de string contenant le nom complet des fichiers dfs (les seuls dont le nom peut varier
        """
        print('upload sur ftp ...')
        texteFichierSuivi=''
        fichiers =['FR_A_MRoad','FR_A_MRail']
        for fichier in fichiers : #un moyen poutr telecharger les fihciers SIg des sosurces et cartes, il doit y en avoir d'autre
            for suffixe in ['_Source','_Lden','_Ln'] : 
                try : 
                    for j in ('.shp', '.shx', '.dbf', '.prj'):
                        self.instanceSsh.sftp.put(os.path.join(self.DossierCartesDeCopie, fichier+suffixe+j),'Projet_Reussir_2017_CBS_et_PPBE/rapportage'+'/'+fichier+suffixe+j)
                    texteFichierSuivi+='upload des fichiers '+fichier+' sur ftp ok \n'
                except : 
                    texteFichierSuivi+='erreur upload des fichiers '+fichier+' sur ftp \n'
        for df in  listeFichierDf: #teclechragement des fichiers excel
            try :
                self.instanceSsh.sftp.put(os.path.join(self.DossierCartesDeCopie,df),'Projet_Reussir_2017_CBS_et_PPBE/rapportage/'+df)
                texteFichierSuivi+='upload du fichier '+df+' sur ftp ok \n'
            except :
                texteFichierSuivi+='erreur upload du fichier '+df+' sur ftp \n'
        try: # telchargement du fichier de suivi des pb du present module
            self.instanceSsh.sftp.put(os.path.join(self.DossierCartesDeCopie,'tableauSuiviCreationRapportage.csv'),'Projet_Reussir_2017_CBS_et_PPBE/rapportage/'+'tableauSuiviCreationRapportage.csv')
            texteFichierSuivi+='upload du tableur de suivi sur ftp ok \n'
        except : 
            texteFichierSuivi+='erreur upload du tableur de suivi sur ftp \n'
        
        self.listeFichierSuivi[len(self.listeFichierSuivi)-1].append(texteFichierSuivi)
        print ('fait')
               
    def executerRapportage(self):
        """
        Pour enchainer les actions des fonction de recuperation des donnÃ©es df1548, cartes et lineaire
        pas de paramÃ¨tres en entrÃ©e, utilise la liste de departement qui Ã©dfinit l'instance
        """
        #for dept in self.listeDepartement:
            #aexecuterTransfert=self.transfertDf1548(dept) #on realise la fonction de transfert, qui indique s'il y a des donnes de rapportage ou non
            #if not executerTransfert : #si pas de donnees
                #continue #on passe au dept suivant
            #self.transfertCartes(dept)
            #self.creationUueid(dept) 
            #self.creerCodeDf710(dept) 
            #self.affectationLineaire(dept)#a verifier le cas d'une mÃªme route avec plusieurs gestionnaire : l'affectation pourrait etre foireuse
        
        #self.creationLineaire()
        #self.creerLignesSources()
        listeFichierSortie=self.exporterTableurs(r'E:\Boulot\rapportage',['DF1_5','DF4_8','DF7_10']) #on execute la focntion qui fait le transfert, et en sortie on recupere le nom des dfs crees
        #self.creerNoiseContours()
        #self.uploadRapportageSurFtp(listeFichierSortie)
        #self.creerFichierSuivi()
        print("fini sans erreur")
        self.instanceSsh.close() 

class RapportageAgglo(Connexion_Transfert.ConnexionBdd):
    """
    Classe pour rapporter les CBS Ã  l'Union europÃ©enne
    nÃ©cessite :
    une connexion Ã  une Bdd postgres contenant les tables rapportage.df1548710_rail_e3,rapportage.df1548710_road_e3  et cartes_bruit.n_bruit_zone_s
    des modeles de fichiers de l'Eionet
    """
    
    def __init__(self, parent=None):
        super().__init__()#recuperation du constructeur de la classe mere
        self.transfertOgr=Connexion_Transfert.Ogr2Ogr()
    
    # 1 parcourir D:\temp\rapportage\agglo ou se connecter au SFTP et obtenir la liste des fichiers avec le chemin (fonction walk ou sftp.walk) et en obtenir une liste
    def executerRapportage(self):
        """for (dirpath,dirname,files) in os.walk(r'D:\temp\rapportage\agglo') :
            for file in files : 
                #extraction des données de dénombrement
                if (file.endswith('.ods')): #extraction des données dans une liste propre
                    data=pyexcel_ods.get_data(os.path.join(dirpath,file))
                    nomFeuille1=list(data.keys())[0]
                    donneesFeuille1=data[nomFeuille1]
                    donneesFeuille1=list(filter(None,donneesFeuille1))[3]#je pars du principe qu'il n'y aqu'une seule ligne de données
                
                    #affcetation des variables: 
                    aggloName=donneesFeuille1[1]
                    uueid=donneesFeuille1[2]
                    habitants=donneesFeuille1[3]
                    surface=donneesFeuille1[4]
                    locLau2Codes=[code for code in (donneesFeuille1[5].replace(' ','')).split(',')]#parce que parfois on a des expaces et que le chamsp destinataire est un array
                    routeLden55, routeLden60, routeLden65, routeLden70, routeLden75=donneesFeuille1[6], donneesFeuille1[7], donneesFeuille1[8], donneesFeuille1[9], donneesFeuille1[10]
                    routeLn50, routeLn55, routeLn60, routeLn65, routeLn70 =donneesFeuille1[16], donneesFeuille1[17], donneesFeuille1[18], donneesFeuille1[19], donneesFeuille1[20]
                    ferLden55, ferLden60, ferLden65, ferLden70, ferLden75=donneesFeuille1[26], donneesFeuille1[27], donneesFeuille1[28], donneesFeuille1[29], donneesFeuille1[30]
                    ferLn50, ferLn55, ferLn60, ferLn65, ferLn70 =donneesFeuille1[36], donneesFeuille1[37], donneesFeuille1[38], donneesFeuille1[39], donneesFeuille1[40]
                    airLden55, airLden60, airLden65, airLden70, airLden75=donneesFeuille1[46], donneesFeuille1[47], donneesFeuille1[48], donneesFeuille1[49], donneesFeuille1[50]
                    airLn50, airLn55, airLn60, airLn65, airLn70 =donneesFeuille1[56], donneesFeuille1[57], donneesFeuille1[58], donneesFeuille1[59], donneesFeuille1[60]
                    indusLden55, indusLden60, indusLden65, indusLden70, indusLden75=donneesFeuille1[46], donneesFeuille1[47], donneesFeuille1[48], donneesFeuille1[49], donneesFeuille1[50]
                    indusLn50, indusLn55, indusLn60, indusLn65, indusLn70 =donneesFeuille1[56], donneesFeuille1[57], donneesFeuille1[58], donneesFeuille1[59], donneesFeuille1[60]
                    methode=donneesFeuille1[76]
                    urlPubli=donneesFeuille1[79]
                    print (file, donneesFeuille1)  
                    
                    #mise à jour de postgres selon l'echeance
                    if '2007' in dirpath :
                        self.curs.execute("INSERT INTO rapportage.df1548710_agg_e1 (ctryid, unaggid, agglomerat, inhabitant, size, loclau2cod,air_p55ldag, air_p60ldag, air_p65ldag, air_p70ldag, air_p75ldag,air_p50lnag, air_p55lnag, air_p60lnag, air_p65lnag, air_p70lnag, ind_p55ldag, ind_p60ldag, ind_p65ldag, ind_p70ldag, ind_p75ldag,ind_p50lnag, ind_p55lnag, ind_p60lnag,ind_p65lnag, ind_p70lnag,roa_p55ldag,roa_p60ldag, roa_p65ldag, roa_p70ldag, roa_p75ldag,roa_p50lnag, roa_p55lnag, roa_p60lnag, roa_p65lnag,roa_p70lnag,rai_p55ldag, rai_p60ldag, rai_p65ldag, rai_p70ldag, rai_p75ldag, rai_p50lnag, rai_p55lnag, rai_p60lnag, rai_p65lnag, rai_p70lnag, compmetnam, urlpubli) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", ('A', uueid,aggloName,habitants,surface,locLau2Codes,airLden55, airLden60, airLden65, airLden70, airLden75,airLn50, airLn55, airLn60, airLn65, airLn70,indusLden55, indusLden60, indusLden65, indusLden70, indusLden75,indusLn50, indusLn55, indusLn60, indusLn65, indusLn70,routeLden55, routeLden60, routeLden65, routeLden70, routeLden75,routeLn50, routeLn55, routeLn60, routeLn65, routeLn70,ferLden55, ferLden60, ferLden65, ferLden70, ferLden75,ferLn50, ferLn55, ferLn60, ferLn65, ferLn70,methode,urlPubli))
                    elif '2012' in dirpath :
                        self.curs.execute("INSERT INTO rapportage.df1548710_agg_e2 (ctryid, unaggid, agglomerat, inhabitant, size, loclau2cod,air_p55ldag, air_p60ldag, air_p65ldag, air_p70ldag, air_p75ldag,air_p50lnag, air_p55lnag, air_p60lnag, air_p65lnag, air_p70lnag, ind_p55ldag, ind_p60ldag, ind_p65ldag, ind_p70ldag, ind_p75ldag,ind_p50lnag, ind_p55lnag, ind_p60lnag,ind_p65lnag, ind_p70lnag,roa_p55ldag,roa_p60ldag, roa_p65ldag, roa_p70ldag, roa_p75ldag,roa_p50lnag, roa_p55lnag, roa_p60lnag, roa_p65lnag,roa_p70lnag,rai_p55ldag, rai_p60ldag, rai_p65ldag, rai_p70ldag, rai_p75ldag, rai_p50lnag, rai_p55lnag, rai_p60lnag, rai_p65lnag, rai_p70lnag, compmetnam, urlpubli) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", ('A', uueid,aggloName,habitants,surface,locLau2Codes,airLden55, airLden60, airLden65, airLden70, airLden75,airLn50, airLn55, airLn60, airLn65, airLn70,indusLden55, indusLden60, indusLden65, indusLden70, indusLden75,indusLn50, indusLn55, indusLn60, indusLn65, indusLn70,routeLden55, routeLden60, routeLden65, routeLden70, routeLden75,routeLn50, routeLn55, routeLn60, routeLn65, routeLn70,ferLden55, ferLden60, ferLden65, ferLden70, ferLden75,ferLn50, ferLn55, ferLn60, ferLn65, ferLn70,methode,urlPubli))
                    elif '2017' in dirpath :
                        self.curs.execute("INSERT INTO rapportage.df1548710_agg_e3 (ctryid, unaggid, agglomerat, inhabitant, size, loclau2cod,air_p55ldag, air_p60ldag, air_p65ldag, air_p70ldag, air_p75ldag,air_p50lnag, air_p55lnag, air_p60lnag, air_p65lnag, air_p70lnag, ind_p55ldag, ind_p60ldag, ind_p65ldag, ind_p70ldag, ind_p75ldag,ind_p50lnag, ind_p55lnag, ind_p60lnag,ind_p65lnag, ind_p70lnag,roa_p55ldag,roa_p60ldag, roa_p65ldag, roa_p70ldag, roa_p75ldag,roa_p50lnag, roa_p55lnag, roa_p60lnag, roa_p65lnag,roa_p70lnag,rai_p55ldag, rai_p60ldag, rai_p65ldag, rai_p70ldag, rai_p75ldag, rai_p50lnag, rai_p55lnag, rai_p60lnag, rai_p65lnag, rai_p70lnag, compmetnam, urlpubli) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", ('A', uueid,aggloName,habitants,surface,locLau2Codes,airLden55, airLden60, airLden65, airLden70, airLden75,airLn50, airLn55, airLn60, airLn65, airLn70,indusLden55, indusLden60, indusLden65, indusLden70, indusLden75,indusLn50, indusLn55, indusLn60, indusLn65, indusLn70,routeLden55, routeLden60, routeLden65, routeLden70, routeLden75,routeLn50, routeLn55, routeLn60, routeLn65, routeLn70,ferLden55, ferLden60, ferLden65, ferLden70, ferLden75,ferLn50, ferLn55, ferLn60, ferLn65, ferLn70,methode,urlPubli))
                    self.connexionPsy.commit()
                    
                    print (file, 'fait')"""
        
        fichierCopie=r'D:\temp\rapportage\agglo\FR_B_ag0003\aubergenville\CBS\2007\FR_B_ag0003_Aubergenville_AggIndustry_Lden'
        fichier=Ogr_Perso.DonneesShapefile(fichierCopie+'.shp')
        sql='-sql "SELECT *, \'industrie\' AS typesource FROM '+ fichierCopie+'"'
        print (sql)
        self.transfertOgr.shp2pg(self.connstringOgr, fichierCopie+'.shp',schema="rapportage", table="df48_agg_sig_e1",geotype="MULTIPOLYGON", dims=2,creationMode="-update -append",requeteSql=sql)           
        print ('fini complet')
    # 2 pour chaque fichier de la liste : 
        #si c'est un '.ods' : 
            #déterminer l'échéace selon la présence de 2007 ou 2012 ou 2017 dans le dirname
            #extraire les informations dans une liste
            #affecter les valeurs de cette liste à des variables
            #transférer ces variables dans la table postgres relative à l'échéance
        # si c'est un .shp
            #déterminer l'échéace selon la présence de 2007 ou 2012 ou 2017 dans le dirname
            #déterminer la valeur de l'uueid selon le nom du fihicer
            #insérer dans la table format rappoartege
            #insérer dans la table format GéoStandard
    # 3 exporter depuis la Bdd vers les fichiers au format prévu (cf Gitt)
    
    
    
    
    
if __name__=='__main__': #dans le cas oÃ¹ on execute le module
    app=QApplication(sys.argv)
    
    #RAPPORTAGE GITT
    listeDept=['976'] #por test sur un seul département
    #création de laiste des département
    #listeDept=list(range(1,96))
    #listeDept=['00'+str(i) for i in listeDept if i >0 and i<10]+['0'+str(i) for i in listeDept if i >=10 and i !=20]+['02A','02B']+['971','972','973','974','976']
    print (listeDept)
    instanceRapportage=RapportageGitt(listeDept) #CrÃ©er une instance de rapportage
    #instanceRapportage.serveur='172.22.112.101' 
    instanceRapportage.creerConnexion()#gÃ©nÃ©rer les connexions psycopg et ogr de cette instance
    instanceRapportage.executerRapportage()#et c'est parti
    instanceRapportage=1 #pour supprimer tout lien de l'instance et l'envoyer vers le garbage collector
    """
    #RAPPORTAGE AGGLO
    instanceRapportage=RapportageAgglo()
    instanceRapportage.serveur='172.22.112.101' #sur le pc perso postgres 10 est sur le port 5433
    instanceRapportage.creerConnexion()#gÃ©nÃ©rer les connexions psycopg et ogr de cette instance
    instanceRapportage.executerRapportage()#et c'est parti
    instanceRapportage=1"""
