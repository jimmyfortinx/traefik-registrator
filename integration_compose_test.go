package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestComposeExamples(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping compose integration tests in short mode")
	}
	if os.Getenv("RUN_COMPOSE_TESTS") != "1" {
		t.Skip("set RUN_COMPOSE_TESTS=1 to run compose integration tests")
	}

	composeBin, composePrefix, err := detectComposeCommand()
	if err != nil {
		t.Skipf("compose command unavailable: %v", err)
	}

	files, err := filepath.Glob(filepath.Join("examples", "*", "docker-compose.yml"))
	if err != nil {
		t.Fatalf("list compose examples: %v", err)
	}
	sort.Strings(files)
	if len(files) == 0 {
		t.Fatal("no compose example scenarios found")
	}

	for _, composeFile := range files {
		scenario := filepath.Base(filepath.Dir(composeFile))
		t.Run(scenario, func(t *testing.T) {
			project := composeProjectName(scenario)
			defer func() {
				downCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				_, _ = runCommand(downCtx, composeBin, append(composePrefix,
					"-p", project,
					"-f", composeFile,
					"down", "-v", "--remove-orphans",
				)...)
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
			defer cancel()
			upOut, upErr := runCommand(ctx, composeBin, append(composePrefix,
				"-p", project,
				"-f", composeFile,
				"up",
				"--build",
				"--abort-on-container-exit",
				"--exit-code-from", "verify",
				"--renew-anon-volumes",
			)...)
			if upErr != nil {
				logsCtx, logsCancel := context.WithTimeout(context.Background(), 90*time.Second)
				defer logsCancel()
				logsOut, _ := runCommand(logsCtx, composeBin, append(composePrefix,
					"-p", project,
					"-f", composeFile,
					"logs", "--no-color",
				)...)
				t.Fatalf(
					"scenario failed: %v\n--- up output ---\n%s\n--- logs ---\n%s",
					upErr,
					upOut,
					logsOut,
				)
			}
		})
	}
}

func detectComposeCommand() (string, []string, error) {
	if _, err := exec.LookPath("docker"); err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := exec.CommandContext(ctx, "docker", "compose", "version").Run(); err == nil {
			return "docker", []string{"compose"}, nil
		}
	}
	if _, err := exec.LookPath("docker-compose"); err == nil {
		return "docker-compose", nil, nil
	}
	return "", nil, fmt.Errorf("neither docker compose nor docker-compose is available")
}

func composeProjectName(scenario string) string {
	slug := strings.ToLower(scenario)
	re := regexp.MustCompile(`[^a-z0-9]+`)
	slug = re.ReplaceAllString(slug, "-")
	slug = strings.Trim(slug, "-")
	if slug == "" {
		slug = "example"
	}
	suffix := fmt.Sprintf("%x", time.Now().UnixNano()&0xfffff)
	name := "trreg-" + slug + "-" + suffix
	if len(name) > 55 {
		name = name[:55]
	}
	return strings.Trim(name, "-")
}

func runCommand(ctx context.Context, bin string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = os.Environ()

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return out.String(), err
}
