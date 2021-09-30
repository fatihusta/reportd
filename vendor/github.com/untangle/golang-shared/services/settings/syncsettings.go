package settings

import (
	"bufio"
	"errors"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/untangle/golang-shared/services/logger"
)

// SyncSettings is the struct holding sync-settings information
type SyncSettings struct {
	settingsFile           string
	defaultsFile           string
	currentFile            string
	osForSyncSettings      string
	tmpSettingsFile        string
	syncSettingsExecutable string
	uidFile                string
}

// NewSyncSettings creates a new settings object
func NewSyncSettings(settingsfile string, defaultsfile string, currentfile string, osforsyncsettings string, tmpsettingsfile string, syncsettingsexecutable string, uidfile string) *SyncSettings {
	s := new(SyncSettings)

	s.settingsFile = settingsfile
	s.defaultsFile = defaultsfile
	s.currentFile = currentfile
	s.osForSyncSettings = osforsyncsettings
	s.tmpSettingsFile = tmpsettingsfile
	s.syncSettingsExecutable = syncsettingsexecutable
	s.uidFile = uidfile

	return s

}

// CreateDefaults creates the settings defauls.json file
func (s *SyncSettings) CreateDefaults() error {
	// sync the defaults
	cmdArgs := []string{"-o", s.osForSyncSettings, "-c", "-s", "-f", s.tmpSettingsFile}
	err := s.runSyncSettings(cmdArgs)
	if err != nil {
		logger.Warn("Error creating defaults: %s\n", err.Error())
		return err
	}

	// move the defaults. Have to read/write file to avoid docker copy errors
	defaultsBytes, readErr := ioutil.ReadFile(s.tmpSettingsFile)
	if readErr != nil {
		logger.Warn("Failure copying defaults over: %s\n", readErr.Error())
		return readErr
	}

	writeErr := ioutil.WriteFile(s.defaultsFile, defaultsBytes, 0660)
	if writeErr != nil {
		logger.Warn("Failure copying defaults over: %s\n", writeErr.Error())
		return writeErr
	}

	removeErr := os.Remove(s.tmpSettingsFile)
	if removeErr != nil {
		logger.Warn("Could not remove default tmp file: %s. Continueing\n", removeErr.Error())
	}

	return nil
}

// NormalSync runs sync settings with OS and filename specified
func (s *SyncSettings) NormalSync() error {
	cmdArgs := []string{"-o", s.osForSyncSettings, "-f", s.settingsFile}
	err := s.runSyncSettings(cmdArgs)
	if err != nil {
		logger.Warn("Error running sync-settings: %s\n", err.Error())
		return err
	}
	return nil
}

// FirstSyncSettingsRun will create the settings file if it doesn't exist, or rerun sync-settings for good measure
func (s *SyncSettings) FirstSyncSettingsRun() error {
	cmdArgs := []string{"-o", s.osForSyncSettings, "-n"}

	// check if settings.json exists, if not create it
	info, checkErr := os.Stat(s.settingsFile)
	if os.IsNotExist(checkErr) {
		cmdArgs = append(cmdArgs, "-c")
	} else if info.IsDir() {
		logger.Warn("File is a directory, that's wrong\n")
		return errors.New("Settings file is a directory")
	} else if checkErr != nil {
		logger.Warn("Something went wrong creating settings file: %s\n", checkErr.Error())
		return checkErr
	}

	err := s.runSyncSettings(cmdArgs)
	if err != nil {
		logger.Warn("Error running sync-settings: %s\n", err.Error())
		return err
	}
	return nil
}

// runSyncSettings runs sync settings with given cmd args
func (s *SyncSettings) runSyncSettings(cmdArgs []string) error {
	cmd := exec.Command(s.syncSettingsExecutable, cmdArgs...)
	outbytes, err := cmd.CombinedOutput()
	output := string(outbytes)
	var runErr error
	runErr = nil
	if err != nil {
		// if just a non-zero exit code, just use standard language
		// otherwise use the real error message
		if exiterr, ok := err.(*exec.ExitError); ok {
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				if status.ExitStatus() != 0 {
					logger.Warn("Failed to run sync-settings: %v\n", err.Error())
					runErr = errors.New("Failed to save settings")
				}
			}
		}
		logger.Err("Failed to run sync-settings: %v\n", err.Error())
		runErr = err
	}
	outputErr := s.logSyncSettingsOutput(output, runErr)
	if outputErr != nil {
		return outputErr
	}
	return runErr
}

// logSyncSettingsOutput logs the output from a sync-settings run
func (s *SyncSettings) logSyncSettingsOutput(output string, err error) error {
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		if logger.IsDebugEnabled() {
			logger.Debug("sync-settings: %v\n", scanner.Text())
		}
	}
	if err != nil {
		logger.Warn("sync-settings return an error: %v\n", err.Error())
		return err
	}
	return nil
}
