/*
 * Copyright (C) 2018. OpenLattice, Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.integrations.CambridgePD;

import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import com.openlattice.shuttle.util.Parsers;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.openlattice.client.RetrofitFactory.Environment;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Kim Engie &lt;kim@openlattice.com&gt;
 */

public class Cambridge {
    private static final Logger logger = LoggerFactory.getLogger( Cambridge.class );
    private static final Environment environment = Environment.PRODUCTION;

    private static final DateTimeHelper dtHelper = new DateTimeHelper( TimeZones.America_NewYork, "MM/dd//YY HH:mm", "M/d/YY HH:mm", "M/d/YY H:mm" );
    private static final DateTimeHelper bdHelper = new DateTimeHelper( TimeZones.America_NewYork, "MM/dd/yyyy" );


    public static void main( String[] args ) throws InterruptedException {
        /*
         * It's worth noting that we are omitting validation such as making sure enough args were passed in, checking
         * existence of the file, and making sure authentication was successful. A failure in one of these cases
         * will cause the program to exit with an exception.
         */

        final String incidentsPath = args[ 0 ];
        final String jwtToken = args[ 1 ];

        SimplePayload iPayload = new SimplePayload( incidentsPath );

        List<Map<String, String>> fp = iPayload.getPayload().collect( Collectors.toList() );
        Payload suspectsPayload = new SimplePayload( fp.stream()
                .filter( row -> row.get( "Role" ).equals( "SUSPECT" ) ));
        Payload defendantsPayload = new SimplePayload( fp.stream()
                .filter( row -> row.get( "Role" ).equals( "DEFENDANT" )));
        Payload othersPayload = new SimplePayload( fp.stream()
                .filter( row -> !(row.get( "Role" ).equals( "SUSPECT" )) && !(row.get( "Role" ).equals( "DEFENDANT" ))));


                logger.info( "Using the following idToken: Bearer {}", jwtToken );

                //@formatter:off
        Flight suspectsflight = Flight.newFlight()
                .createEntities()

                .addEntity( "people" )
                    .to( "CambridgePeople_1" )
                    .addProperty( "nc.SubjectIdentification", "PersonID" )
                    .addProperty( "nc.PersonGivenName", "First" )
                    .addProperty( "nc.PersonMiddleName", "Middle" )
                    .addProperty( "nc.PersonSurName", "Last" )
                    .addProperty( "nc.PersonBirthDate" )
                        .value( row -> bdHelper.parseDate( row.getAs( "DOB" ) ) ).ok()
                    .addProperty( "nc.SSN", "SSN" )
                    .addProperty( "nc.PersonSex", "Sex" )
                    .addProperty( "nc.PersonRace" )
                        .value( Cambridge::standardRaceList  ).ok()
                    .addProperty( "nc.PersonEthnicity" )
                        .value( Cambridge::standardEthnicity ).ok()
                .endEntity()

                .addEntity( "suspects" )
                    .to("CambridgeSuspects")
                    .addProperty( "criminaljustice.personid", "PersonID" )
                    .addProperty( "criminaljustice.persontype", "Role" )
                    .addProperty( "nc.PersonHeightMeasure")
                        .value( row -> Parsers.parseInt( row.getAs ("Height") ) ).ok()
                    .addProperty( "nc.PersonWeightMeasure" )
                        .value( row -> Parsers.parseInt( row.getAs ("Weight") ) ).ok()
                .endEntity()

                .addEntity( "paddress" )
                    .to("CambridgeAddresses")
                    .addProperty( "location.Address" )
                        .value( Cambridge::getPAddress ).ok()
                    .addProperty( "location.street" )
                        .value( Cambridge::getPStreet ).ok()
                    .addProperty( "location.apartment", "MasterPerson.Unit" )
                    .addProperty( "location.city", "MasterPerson.City" )
                    .addProperty( "location.state", "State" )
                .endEntity()

                .addEntity( "Iaddress" )
                    .to("CambridgeAddresses")
                    .useCurrentSync()
                    .addProperty( "location.Address" )
                        .value( Cambridge::getIAddress ).ok()
                    .addProperty( "location.street" )
                        .value( Cambridge::getIStreet ).ok()
                    .addProperty( "location.apartment", "MasterPerson.Unit" )
                    .addProperty( "location.city", "MasterPerson.City" )
                    .addProperty( "location.state", "State" )
                .endEntity()

                .addEntity( "incident" )
                    .to( "CambridgePDIncidents" )
                    .addProperty( "criminaljustice.incidentid", "IncNum" )
                    .addProperty( "ol.recordtype", "RecordType" )
                    .addProperty( "criminaljustice.nibrs", "NIBRSOffCode" )
                    .addProperty( "criminaljustice.localstatute", "Offense" )
                    .addProperty( "ol.datetime_reported" )
                        .value( row -> dtHelper.parse( row.getAs ("DTReported")) ).ok()  //LocalDate.now() ).ok()
                    .addProperty( "publicsafety.drugspresent", "DrugOtherActivity" )
                    .addProperty( "ol.gangactivity", "GangActivity" )
                    .addProperty( "publicsafety.weaponspresent", "Weapon" )
                    .addProperty( "ol.domesticviolenceflag", "Domestic" )
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( "becomes")
                    .to( "CambridgeBecomes" )
                    .fromEntity( "people" )
                    .toEntity( "suspects" )
                    .addProperty( "nc.SubjectIdentification", "PersonID" )
                    .endAssociation()
                .addAssociation( "appearsin" )
                    .to( "CambridgeAppearIn")
                    .fromEntity( "suspects" )
                    .toEntity( "incident" )
                    .addProperty( "nc.SubjectIdentification", "PersonID" )
                    .addProperty( "general.stringid", "IncNum" )
                .endAssociation()
                .addAssociation( "locatedat" )
                    .to( "CambridgeLocatedAt" )
                    .fromEntity( "people" )
                    .toEntity( "paddress" )
                    .addProperty( "nc.SubjectIdentification", "PersonID" )
                .endAssociation()
                 .addAssociation( "occurredat" )
                    .to( "CambridgeOccurredAt" )
                    .fromEntity( "incident" )
                    .toEntity( "Iaddress" )
                    .addProperty( "general.stringid", "IncNum" )
                .endAssociation()

                .endAssociations()
                .done();


        Flight defendantsflight = Flight.newFlight()
                .createEntities()

                .addEntity( "people2" )
                    .to( "CambridgePeople_1" )
                    .useCurrentSync()
                    .addProperty( "nc.SubjectIdentification", "PersonID" )
                    .addProperty( "nc.PersonGivenName", "First" )
                    .addProperty( "nc.PersonMiddleName", "Middle" )
                    .addProperty( "nc.PersonSurName", "Last" )
                    .addProperty( "nc.PersonBirthDate" )
                        .value( row -> bdHelper.parseDate( row.getAs( "DOB" ) ) ).ok()
                    .addProperty( "nc.SSN", "SSN" )
                    .addProperty( "nc.PersonSex", "Sex" )
                    .addProperty( "nc.PersonRace" )
                        .value( Cambridge::standardRaceList  ).ok()
                    .addProperty( "nc.PersonEthnicity" )
                        .value( Cambridge::standardEthnicity ).ok()
                .endEntity()

                .addEntity( "defendants" )
                    .to("CambridgeSuspects")
                    .useCurrentSync()
                    .addProperty( "criminaljustice.personid", "PersonID" )
                    .addProperty( "criminaljustice.persontype", "Role" )
                    .addProperty( "nc.PersonHeightMeasure")
                        .value( row -> Parsers.parseInt( row.getAs ("Height") ) ).ok()
                    .addProperty( "nc.PersonWeightMeasure" )
                        .value( row -> Parsers.parseInt( row.getAs ("Weight") ) ).ok()
                .endEntity()

                .addEntity( "paddress2" )
                    .to("CambridgeAddresses")
                    .useCurrentSync()
                    .addProperty( "location.Address" )
                        .value( Cambridge::getPAddress ).ok()
                    .addProperty( "location.street" )
                        .value( Cambridge::getPStreet ).ok()
                    .addProperty( "location.apartment", "MasterPerson.Unit" )
                    .addProperty( "location.city", "MasterPerson.City" )
                    .addProperty( "location.state", "State" )
                .endEntity()

                .addEntity( "Iaddress2" )
                    .to("CambridgeAddresses")
                    .useCurrentSync()
                    .addProperty( "location.Address" )
                        .value( Cambridge::getIAddress ).ok()
                    .addProperty( "location.street" )
                        .value( Cambridge::getIStreet ).ok()
                    .addProperty( "location.apartment", "MasterPerson.Unit" )
                    .addProperty( "location.city", "MasterPerson.City" )
                    .addProperty( "location.state", "State" )
                .endEntity()

                .addEntity( "incident2" )
                    .to( "CambridgePDIncidents" )
                    .useCurrentSync()
                    .addProperty( "criminaljustice.incidentid", "IncNum" )
                    .addProperty( "ol.recordtype", "RecordType" )
                    .addProperty( "criminaljustice.nibrs", "NIBRSOffCode" )
                    .addProperty( "criminaljustice.localstatute", "Offense" )
                    .addProperty( "ol.datetime_reported" )
                        .value( row -> dtHelper.parse( row.getAs ("DTReported")) ).ok()  //LocalDate.now() ).ok()
                    .addProperty( "publicsafety.drugspresent", "DrugOtherActivity" )
                    .addProperty( "ol.gangactivity", "GangActivity" )
                    .addProperty( "publicsafety.weaponspresent", "Weapon" )
                    .addProperty( "ol.domesticviolenceflag", "Domestic" )
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( "becomes2")
                    .to( "CambridgeBecomes" )
                    .useCurrentSync()
                    .fromEntity( "people2" )
                    .toEntity( "defendants" )
                    .addProperty( "nc.SubjectIdentification", "PersonID" )
                    .endAssociation()
                .addAssociation( "arrestedin" )
                    .to( "CambridgeArrestedIn")
                    .fromEntity( "defendants" )
                    .toEntity( "incident2" )
                    .addProperty( "nc.SubjectIdentification", "PersonID" )
                    .addProperty( "arrestedin.id", "IncNum" )
                .endAssociation()
                .addAssociation( "locatedat2" )
                    .to( "CambridgeLocatedAt" )
                    .useCurrentSync()
                    .fromEntity( "people2" )
                    .toEntity( "paddress2" )
                    .addProperty( "nc.SubjectIdentification", "PersonID" )
                .endAssociation()
                 .addAssociation( "occurredat2" )
                    .to( "CambridgeOccurredAt" )
                    .useCurrentSync()
                    .fromEntity( "incident2" )
                    .toEntity( "Iaddress2" )
                    .addProperty( "general.stringid", "IncNum" )
                .endAssociation()

                .endAssociations()
                .done();

        Flight othersflight = Flight.newFlight()
                .createEntities()

                .addEntity( "people3" )
                    .to( "CambridgePeople_2" )
                    .addProperty( "nc.SubjectIdentification", "PersonID" )
                    .addProperty( "nc.PersonGivenName", "First" )
                    .addProperty( "nc.PersonMiddleName", "Middle" )
                    .addProperty( "nc.PersonSurName", "Last" )
                    .addProperty( "nc.PersonBirthDate" )
                        .value( row -> bdHelper.parseDate( row.getAs( "DOB" ) ) ).ok()
                    .addProperty( "nc.SSN", "SSN" )
                    .addProperty( "nc.PersonSex", "Sex" )
                    .addProperty( "nc.PersonRace" )
                        .value( Cambridge::standardRaceList  ).ok()
                    .addProperty( "nc.PersonEthnicity" )
                        .value( Cambridge::standardEthnicity ).ok()
                .endEntity()

                .addEntity( "JIpeople" )
                    .to("CambridgeJusticePeople")
                    .addProperty( "criminaljustice.personid", "PersonID" )
                    .addProperty( "criminaljustice.persontype", "Role" )
                    .addProperty( "nc.PersonHeightMeasure")
                        .value( row -> Parsers.parseInt( row.getAs ("Height") ) ).ok()
                    .addProperty( "nc.PersonWeightMeasure")
                        .value( row -> Parsers.parseInt( row.getAs ("Weight") ) ).ok()
                .endEntity()

                .addEntity( "paddress3" )
                    .to("CambridgeAddresses")
                    .useCurrentSync()
                    .addProperty( "location.Address" )
                        .value( Cambridge::getPAddress).ok()
                    .addProperty( "location.street" )
                        .value( Cambridge::getPStreet ).ok()
                    .addProperty( "location.apartment", "MasterPerson.Unit" )
                    .addProperty( "location.city", "MasterPerson.City" )
                    .addProperty( "location.state", "State" )
                .endEntity()

                .addEntity( "Iaddress3" )
                    .to("CambridgeAddresses")
                    .useCurrentSync()
                    .addProperty( "location.Address" )
                        .value( Cambridge::getIAddress).ok()
                    .addProperty( "location.street" )
                        .value( Cambridge::getIStreet ).ok()
                    .addProperty( "location.apartment", "MasterPerson.Unit" )
                    .addProperty( "location.city", "MasterPerson.City" )
                    .addProperty( "location.state", "State" )
                .endEntity()

                .addEntity( "incident3" )
                    .to( "CambridgePDIncidents" )
                    .useCurrentSync()
                    .addProperty( "criminaljustice.incidentid", "IncNum" )
                    .addProperty( "ol.recordtype", "RecordType" )
                    .addProperty( "criminaljustice.nibrs", "NIBRSOffCode" )
                    .addProperty( "criminaljustice.localstatute", "Offense" )
                    .addProperty( "ol.datetime_reported" )
                        .value( row -> dtHelper.parse( row.getAs( "DTReported" ) ) ).ok()
                    .addProperty( "publicsafety.drugspresent", "DrugOtherActivity" )
                    .addProperty( "ol.gangactivity", "GangActivity" )
                    .addProperty( "publicsafety.weaponspresent", "Weapon" )
                    .addProperty( "ol.domesticviolenceflag", "Domestic" )
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( "becomes3")
                    .to( "CambridgeBecomes" )
                    .fromEntity( "people3" )
                    .toEntity( "JIpeople" )
                    .useCurrentSync()
                    .addProperty( "nc.SubjectIdentification", "PersonID" )
                    .endAssociation()
                .addAssociation( "appearin2" )
                    .to( "CambridgeAppearIn")
                    .useCurrentSync()
                    .fromEntity( "JIpeople" )
                    .toEntity( "incident3" )
                    .addProperty( "general.stringid", "PersonID" )
                    .addProperty( "nc.SubjectIdentification", "PersonID" )
                .endAssociation()
                .addAssociation( "locatedat3" )
                    .to( "CambridgeLocatedAt" )
                    .useCurrentSync()
                    .fromEntity( "people3" )
                    .toEntity( "paddress3" )
                    .addProperty( "nc.SubjectIdentification", "PersonID" )
                .endAssociation()
                 .addAssociation( "occurredat3" )
                    .to( "CambridgeOccurredAt" )
                    .fromEntity( "incident3" )
                    .toEntity( "Iaddress3" )
                    .useCurrentSync()
                    .addProperty( "general.stringid", "IncNum" )
                .endAssociation()

                .endAssociations()
                .done();
                //@formatter:on


        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Payload> flights = new HashMap<>( 1 );
        flights.put( suspectsflight, suspectsPayload );
        flights.put( defendantsflight, defendantsPayload );
        flights.put( othersflight, othersPayload );

        shuttle.launchPayloadFlight( flights );
    }


    public static List standardRaceList( Row row ) {
        String sr = row.getAs("Race");

        if  (sr != null) {

            String[] racesArray = StringUtils.split( sr, "," );
            List<String> races = Arrays.asList( racesArray );

            if ( races != null ) {
                Collections.replaceAll( races, "A", "asian" );
                Collections.replaceAll( races, "W", "white" );
                Collections.replaceAll( races, "B", "black" );
                Collections.replaceAll( races, "I", "amindian" );
                Collections.replaceAll( races, "U", "pacisland" );
                Collections.replaceAll( races, "", "" );

                List<String> finalRaces = races
                        .stream()
                        .filter( StringUtils::isNotBlank )
                        //                    .filter( (race) ->
                        //                            (race != "Client refused") && (race != "Data not collected")
                        //                    )
                        .collect( Collectors.toList() );

                return finalRaces;
            }
            return null;
        }
        return null;
    }

    public static List standardEthnicity( Row row ) {
        String sr = row.getAs("Ethnicity");

        if  (sr != null) {

            String[] racesArray = StringUtils.split( sr, "," );
            List<String> races = Arrays.asList( racesArray );

            if ( races != null ) {
                Collections.replaceAll( races, "N", "nonhispanic" );
                Collections.replaceAll( races, "H", "hispanic" );
                Collections.replaceAll( races, "U", "" );
                Collections.replaceAll( races, "", "" );

                List<String> finalRaces = races
                        .stream()
                        .filter( StringUtils::isNotBlank )
                        .collect( Collectors.toList() );

                return finalRaces;
            }
            return null;
        }
        return null;
    }

    public static String getPStreet( Row row) {
        String unit = Parsers.getAsString( row.getAs( "MasterPerson.Unit" ) );
        String number = Parsers.getAsString( row.getAs( "MasterPerson.StNo" ) );
        String street = Parsers.getAsString( row.getAs( "MasterPerson.Street" ) );

        if ( StringUtils.isNotBlank( unit ) && StringUtils.isNotBlank( number ) && StringUtils.isNotBlank( street ) ) {
            if ( StringUtils.isNotBlank( unit ) ) {
                StringBuilder streetaddress = new StringBuilder( "#" );
                streetaddress.append( unit ).append( ", " ).append( number ).append( " " ).append( street );
                return streetaddress.toString();
            }
            StringBuilder streetaddress = new StringBuilder( number );
            streetaddress.append( " " ).append( street );
            return streetaddress.toString();
        }
        return null;
    }

    public static String getIStreet( Row row) {
        String unit = Parsers.getAsString( row.getAs( "Incidents.Unit" ) );
        String number = Parsers.getAsString( row.getAs( "Incidents.StNo" ) );
        String street = Parsers.getAsString( row.getAs( "Incidents.Street" ) );

        if ( StringUtils.isNotBlank( unit ) && StringUtils.isNotBlank( number ) && StringUtils.isNotBlank( street ) ) {

            if ( StringUtils.isNotBlank( unit ) ) {
                StringBuilder streetaddress = new StringBuilder( "#" );
                streetaddress.append( unit ).append( ", " ).append( number ).append( " " ).append( street );
                return streetaddress.toString();
            }
            StringBuilder streetaddress = new StringBuilder( number );
            streetaddress.append( " " ).append( street );
            return streetaddress.toString();
        }
        return null;
    }

    public static String getIAddress( Row row) {
        String street = Cambridge.getIStreet( row );
        String street2 = Parsers.getAsString( row.getAs( "Street2" ) );
        String city = Parsers.getAsString( row.getAs( "Incidents.City" ) );

        if ( StringUtils.isNotBlank( street ) ) {
            StringBuilder fulladdress = new StringBuilder( street ) ;
                    fulladdress.append(", ").append( street2 ).append( ", " ).append( city ).append( ", MA" );
                    return fulladdress.toString();
        }
        return null;
    }

    public static String getPAddress( Row row) {
        String street = Cambridge.getPStreet( row )  ;
        String city = Parsers.getAsString( row.getAs( "MasterPerson.City" ) );
        String state = Parsers.getAsString( row.getAs( "State" ) );

        if ( StringUtils.isNotBlank( street) ) {
            StringBuilder fulladdress = new StringBuilder( street ) ;
            fulladdress.append(", ").append( city ).append( ", " ).append( state );
            return fulladdress.toString();
        }
        return null;
    }

}
