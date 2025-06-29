const { getDefaultConfig } = require("expo/metro-config");
const { withMonicon } = require("@monicon/metro");
 
const config = getDefaultConfig(__dirname);
 
const configWithMonicon = withMonicon(config, {
  icons: [
  ],
  // Load all icons from the listed collections
  collections: ["solar", "material-symbols"],
});
 
module.exports = configWithMonicon;