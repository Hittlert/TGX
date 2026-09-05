package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/Hittlert/TGX/app/daemon"
)

func NewConflict() *cobra.Command {
	var baseURL string
	var token string
	var jsonOutput bool

	cmd := &cobra.Command{
		Use:     "conflict",
		Aliases: []string{"conflicts"},
		Short:   "Manage and resolve target file conflicts",
	}

	cmd.PersistentFlags().StringVar(&baseURL, "url", "http://127.0.0.1:8080", "base URL of running TGX daemon")
	cmd.PersistentFlags().StringVar(&token, "token", "", "operator auth token or password")
	cmd.PersistentFlags().BoolVar(&jsonOutput, "json", false, "output in JSON format")

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List all download records in conflict state",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client := &http.Client{Timeout: 5 * time.Second}
			req, err := http.NewRequest(http.MethodGet, baseURL+"/api/conflicts", nil)
			if err != nil {
				return fmt.Errorf("create request: %w", err)
			}
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			resp, err := client.Do(req)
			if err != nil {
				return fmt.Errorf("request conflicts: %w", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("daemon returned %d: %s", resp.StatusCode, string(body))
			}

			var records []daemon.DownloadRecord
			if err := json.NewDecoder(resp.Body).Decode(&records); err != nil {
				return fmt.Errorf("decode response: %w", err)
			}

			if jsonOutput {
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				return enc.Encode(records)
			}

			if len(records) == 0 {
				fmt.Println("No conflicted downloads found.")
				return nil
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "CHAT ID\tMSG ID\tFILE NAME\tSIZE\tERROR\tSAVE PATH")
			for _, r := range records {
				fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%s\t%s\n",
					r.ChatID, r.MessageID, r.FileName, r.FileSize, r.Error, r.SavePath)
			}
			return w.Flush()
		},
	}

	var chatFlag string
	var msgFlag int

	resolveCmd := &cobra.Command{
		Use:   "resolve [--chat <chat_id>] [--msg <message_id>] | [flags] -- <chat_id> <message_id>",
		Short: "Reset a conflicted record to pending and trigger retry without restarting daemon",
		RunE: func(cmd *cobra.Command, args []string) error {
			chatID := chatFlag
			msgID := msgFlag

			if chatID == "" && len(args) > 0 {
				chatID = args[0]
			}
			if msgID == 0 && len(args) > 1 {
				parsed, err := strconv.Atoi(args[1])
				if err != nil {
					return fmt.Errorf("invalid message_id %q: must be integer", args[1])
				}
				msgID = parsed
			}

			if chatID == "" || msgID == 0 {
				return fmt.Errorf("chat_id and message_id are required (use --chat and --msg, or '--' before negative channel IDs)")
			}

			payload, _ := json.Marshal(map[string]any{
				"chat_id":    chatID,
				"message_id": msgID,
			})

			client := &http.Client{Timeout: 5 * time.Second}
			req, err := http.NewRequest(http.MethodPost, baseURL+"/api/conflicts/resolve", bytes.NewReader(payload))
			if err != nil {
				return fmt.Errorf("create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}

			resp, err := client.Do(req)
			if err != nil {
				return fmt.Errorf("request resolve: %w", err)
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("daemon returned %d: %s", resp.StatusCode, string(body))
			}

			if jsonOutput {
				fmt.Println(string(body))
				return nil
			}
			fmt.Printf("Successfully requested conflict resolution for chat %s message %d.\n", chatID, msgID)
			return nil
		},
	}
	resolveCmd.Flags().StringVar(&chatFlag, "chat", "", "target channel or chat ID")
	resolveCmd.Flags().IntVar(&msgFlag, "msg", 0, "target message ID")

	cmd.AddCommand(listCmd, resolveCmd)
	return cmd
}
