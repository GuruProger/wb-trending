package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStopList_Add_And_IsBlocked(t *testing.T) {
	sl := NewStopList()

	// Изначально ничего не заблокировано
	assert.False(t, sl.IsBlocked("iphone"), "Слово не должно быть заблокировано изначально")

	// Добавляем слово
	sl.Add("iphone")
	assert.True(t, sl.IsBlocked("iphone"), "Добавленное слово должно быть заблокировано")
	assert.False(t, sl.IsBlocked("samsung"), "Другие слова не должны быть заблокированы")
}

func TestStopList_Remove(t *testing.T) {
	sl := NewStopList()

	sl.Add("iphone")
	require.True(t, sl.IsBlocked("iphone"))

	// Удаляем слово
	sl.Remove("iphone")
	assert.False(t, sl.IsBlocked("iphone"), "Удаленное слово не должно быть заблокировано")

	// Удаление несуществующего слова не должно паниковать
	assert.NotPanics(t, func() {
		sl.Remove("nonexistent")
	})
}

func TestStopList_Add_Duplicate(t *testing.T) {
	sl := NewStopList()

	// Добавляем одно и то же слово дважды
	sl.Add("iphone")
	sl.Add("iphone")

	assert.True(t, sl.IsBlocked("iphone"))

	// Удаляем один раз - должно разблокироваться
	sl.Remove("iphone")
	assert.False(t, sl.IsBlocked("iphone"))
}

func TestStopList_GetAll(t *testing.T) {
	sl := NewStopList()

	// Пустой список
	words := sl.GetAll()
	assert.Empty(t, words)

	// Добавляем несколько слов
	sl.Add("iphone")
	sl.Add("samsung")
	sl.Add("xiaomi")

	words = sl.GetAll()
	assert.Len(t, words, 3)
	assert.Contains(t, words, "iphone")
	assert.Contains(t, words, "samsung")
	assert.Contains(t, words, "xiaomi")
}

func TestStopList_ConcurrentAccess(t *testing.T) {
	sl := NewStopList()
	var wg sync.WaitGroup

	// Запускаем 100 горутин, которые одновременно добавляют и читают
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			word := "word"
			sl.Add(word)
			_ = sl.IsBlocked(word)
			sl.Remove(word)
		}(i)
	}

	wg.Wait()
	// Если дошли сюда без паники и deadlock - тест пройден
}

func TestStopList_CaseSensitive(t *testing.T) {
	sl := NewStopList()

	sl.Add("IPHONE")

	// Проверяем case sensitivity
	assert.True(t, sl.IsBlocked("IPHONE"))
	assert.False(t, sl.IsBlocked("iphone"), "Стоп-лист должен быть case-sensitive")
	assert.False(t, sl.IsBlocked("iPhone"))
}

func TestStopList_EmptyString(t *testing.T) {
	sl := NewStopList()

	// Пустая строка тоже может быть заблокирована
	sl.Add("")
	assert.True(t, sl.IsBlocked(""))

	sl.Remove("")
	assert.False(t, sl.IsBlocked(""))
}

func TestStopList_SpecialCharacters(t *testing.T) {
	sl := NewStopList()

	specialWords := []string{
		"iphone 17",
		"кроссовки!!!",
		"test@#$%",
		"  spaces  ",
	}

	for _, word := range specialWords {
		sl.Add(word)
		assert.True(t, sl.IsBlocked(word), "Слово со спецсимволами должно блокироваться")
	}
}
