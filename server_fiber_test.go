package websocket

import (
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestFiberReadLimit(t *testing.T) {
	s, got := newLimitServer()
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	s.FiberHandler(app)
	assertReadLimit(t, startFiber(t, app)+DefaultWSPath, got)
}

func TestFiberCloseWithCode(t *testing.T) {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	newCloseServer().FiberHandler(app)
	assertCloseFrame(t, startFiber(t, app)+DefaultWSPath)
}

func TestFiberCloseWithInsideOnConnect(t *testing.T) {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	newCloseInOnConnectServer().FiberHandler(app)
	assertCloseFrame(t, startFiber(t, app)+DefaultWSPath)
}
