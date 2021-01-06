Quickstart guide:

1. Set the emulator host environment variable:  
  export SPANNER_EMULATOR_HOST=localhost:9010
2. Start the emulator:  
  docker run -p 9010:9010 --rm -it \<image\>
3. Run the Spanner samples:  
  java -jar java-samples-1.0-SNAPSHOT.jar \<operation\> \<instance\> \<database\>

List of Sample Operations (\<operation\> \<instance\> \<database\>):
    createdatabase my-instance example-db  
    write my-instance example-db  
    delete my-instance example-db  
    query my-instance example-db  
    read my-instance example-db  
    addmarketingbudget my-instance example-db  
    update my-instance example-db  
    writetransaction my-instance example-db  
    querymarketingbudget my-instance example-db  
    addindex my-instance example-db  
    readindex my-instance example-db  
    queryindex my-instance example-db  
    addstoringindex my-instance example-db  
    readstoringindex my-instance example-db  
    readonlytransaction my-instance example-db  
    readstaledata my-instance example-db  
    addcommittimestamp my-instance example-db  
    updatewithtimestamp my-instance example-db  
    querywithtimestamp my-instance example-db  
    createtablewithtimestamp my-instance example-db  
    writewithtimestamp my-instance example-db  
    querysingerstable my-instance example-db  
    queryperformancestable my-instance example-db  
    writestructdata my-instance example-db  
    querywithstruct my-instance example-db  
    querywitharrayofstruct my-instance example-db  
    querystructfield my-instance example-db  
    querynestedstructfield my-instance example-db  
    insertusingdml my-instance example-db  
    updateusingdml my-instance example-db  
    deleteusingdml my-instance example-db  
    updateusingdmlwithtimestamp my-instance example-db  
    writeandreadusingdml my-instance example-db  
    updateusingdmlwithstruct my-instance example-db  
    writeusingdml my-instance example-db  
    querywithparameter my-instance example-db  
    writewithtransactionusingdml my-instance example-db  
    updateusingpartitioneddml my-instance example-db  
    deleteusingpartitioneddml my-instance example-db  
    updateusingbatchdml my-instance example-db  
    createtablewithdatatypes my-instance example-db  
    writedatatypesdata my-instance example-db  
    querywitharray my-instance example-db  
    querywithbool my-instance example-db  
    querywithbytes my-instance example-db  
    querywithdate my-instance example-db  
    querywithfloat my-instance example-db  
    querywithint my-instance example-db  
    querywithstring my-instance example-db  
    querywithtimestampparameter my-instance example-db  
    clientwithqueryoptions my-instance example-db  
    querywithqueryoptions my-instance example-db  

