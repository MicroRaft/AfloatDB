/*
 * Copyright (c) 2020, MicroRaft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.afloatdb;

import com.google.common.base.Charsets;
import com.typesafe.config.ConfigFactory;
import io.afloatdb.config.AfloatDBConfig;

import java.io.IOException;
import java.nio.file.Paths;

import static java.nio.file.Files.readAllLines;

public class RunAfloatDB {

    public static void main(String[] args) {
        String configFileName = getConfigFileName(args);
        System.out.println("Reading config from " + configFileName);

        AfloatDBConfig config = readConfigFile(configFileName);
        AfloatDB server = AfloatDB.bootstrap(config);
        server.awaitTermination();
    }

    private static String getConfigFileName(String[] args) {
        String prop = System.getProperty("afloatdb.config");
        if (args.length == 1 && prop == null) {
            return args[0];
        } else if (args.length == 0 && prop != null) {
            return prop;
        } else {
            System.err.println("Config file name must be provided either via program argument or system parameter: "
                    + "\"afloatdb.config\"!");
            System.exit(-1);
            return null;
        }
    }

    private static AfloatDBConfig readConfigFile(String configFileName) {
        try {
            String config = String.join("\n", readAllLines(Paths.get(configFileName), Charsets.UTF_8));
            return AfloatDBConfig.from(ConfigFactory.parseString(config));
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Cannot read config file: " + configFileName);
            System.exit(-1);
            return null;
        }
    }

}
